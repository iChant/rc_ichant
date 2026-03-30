import logging
import random
import threading
from datetime import datetime, timedelta

import httpx

from notification.config import settings
from notification.models import NotificationJob
from notification.queue import SQLiteTaskQueue

logger = logging.getLogger(__name__)


class Dispatcher:
    """
    单线程后台派发器。

    运行在独立的 daemon 线程中，定期从数据库轮询待投递任务并逐一执行 HTTP 投递。
    单线程设计消除了并发竞争，SQLite 文件锁足以保证数据安全。

    生命周期：
        dispatcher.start()  # 应用启动时调用
        dispatcher.stop()   # 应用关闭时调用（等待当前批次完成）
    """

    def __init__(
        self,
        queue: SQLiteTaskQueue,
        poll_interval: float | None = None,
        batch_size: int | None = None,
        stale_timeout_minutes: int | None = None,
        retry_delay_fn=None,
    ) -> None:
        self._queue = queue
        self._poll_interval = poll_interval or settings.poll_interval_seconds
        self._batch_size = batch_size or settings.batch_size
        self._stale_timeout = (
            stale_timeout_minutes or settings.stale_timeout_minutes
        )
        # 可注入自定义重试延迟函数，主要用于测试（避免等待分钟级的真实退避时间）。
        # 签名：(retry_number: int) -> datetime
        self._retry_delay_fn = (
            retry_delay_fn if retry_delay_fn is not None else _calc_retry_at
        )

        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="dispatcher",
            daemon=True,  # 主进程退出时不等待此线程
        )

    # ── 生命周期 ──────────────────────────────────────────────────────────────

    def start(self) -> None:
        """启动 Dispatcher：执行启动恢复，然后开始轮询。"""
        recovered = self._queue.recover_stale(self._stale_timeout)
        logger.info(
            "Dispatcher starting (poll_interval=%.1fs, batch_size=%d). "
            "Recovered %d stale job(s).",
            self._poll_interval,
            self._batch_size,
            recovered,
        )
        self._thread.start()

    def stop(self) -> None:
        """停止 Dispatcher，等待当前轮次完成（最多 10 秒）。"""
        logger.info("Dispatcher stopping...")
        self._stop_event.set()
        self._thread.join(timeout=10)
        if self._thread.is_alive():
            logger.warning(
                "Dispatcher thread did not stop cleanly within timeout."
            )
        else:
            logger.info("Dispatcher stopped.")

    # ── 主循环 ────────────────────────────────────────────────────────────────

    def _run(self) -> None:
        """派发器主循环：轮询 → 逐一投递 → 等待下次轮询。"""
        while not self._stop_event.is_set():
            try:
                jobs = self._queue.poll(self._batch_size)
                if jobs:
                    logger.info("Dispatching %d job(s).", len(jobs))
                    for job in jobs:
                        if self._stop_event.is_set():
                            # 收到停止信号，结束当前批次（已标记为 processing 的任务
                            # 会在下次启动时由 recover_stale 重置为 pending）
                            break
                        self._deliver(job)
            except Exception:
                logger.exception("Unexpected error in dispatcher loop.")

            # 等待下次轮询，stop() 时可提前唤醒
            self._stop_event.wait(self._poll_interval)

    # ── 单次投递 ──────────────────────────────────────────────────────────────

    def _deliver(self, job: NotificationJob) -> None:
        """
        执行单个任务的 HTTP 投递。

        成功（2xx）→ ack
        失败（非 2xx / 超时 / 网络错误）→ nack 或 dead
        """
        import json

        try:
            headers = json.loads(job.headers)
        except (json.JSONDecodeError, TypeError):
            logger.warning(
                "Job %s has invalid headers JSON, falling back to empty headers.",
                job.id,
            )
            headers = {}

        # 透传幂等键，供 Customer 端实现幂等去重
        if job.idempotency_key:
            headers["X-Idempotency-Key"] = job.idempotency_key

        logger.info(
            "Delivering job %s → %s %s (attempt %d/%d).",
            job.id,
            job.http_method,
            job.target_url,
            job.attempt_count + 1,
            job.max_attempts,
        )

        try:
            response = httpx.request(
                method=job.http_method,
                url=job.target_url,
                headers=headers,
                content=job.body.encode("utf-8") if job.body else None,
                timeout=float(settings.default_timeout_seconds),
            )

            if response.is_success:
                logger.info(
                    "Job %s delivered successfully (HTTP %d).",
                    job.id,
                    response.status_code,
                )
                self._queue.ack(job.id)
            else:
                error = f"HTTP {response.status_code}: {response.text[:500]}"
                logger.warning("Job %s delivery failed: %s", job.id, error)
                self._handle_failure(job, error)

        except httpx.TimeoutException as exc:
            self._handle_failure(job, f"Timeout: {exc}")
        except httpx.RequestError as exc:
            self._handle_failure(job, f"Request error: {exc}")
        except Exception as exc:
            # 捕获意外异常，防止派发器崩溃
            logger.exception("Unexpected error delivering job %s.", job.id)
            self._handle_failure(job, f"Unexpected error: {exc}")

    def _handle_failure(self, job: NotificationJob, error: str) -> None:
        """
        处理投递失败：判断是否超出重试次数，决定 nack 或 dead。

        attempt_count 记录已失败的次数（当前失败后加一）。
        next_attempt = attempt_count + 1 即为本次失败后的累计失败次数。
        若 next_attempt >= max_attempts，进入 dead 状态。
        """
        next_attempt = job.attempt_count + 1

        if next_attempt >= job.max_attempts:
            self._queue.dead(job.id, error)
            logger.error(
                "Job %s exhausted all %d attempt(s) and entered DEAD state.",
                job.id,
                next_attempt,
            )
            # TODO: 集成外部告警（钉钉、PagerDuty 等）
            #   _send_alert(job, error)
        else:
            retry_at = self._retry_delay_fn(next_attempt)
            self._queue.nack(job.id, retry_at, error)
            logger.info(
                "Job %s will retry at %s (attempt %d/%d).",
                job.id,
                retry_at.isoformat(),
                next_attempt,
                job.max_attempts,
            )


# ─────────────────────────────────────────────────────────────────────────────
# 重试时间计算
# ─────────────────────────────────────────────────────────────────────────────


def _calc_retry_at(retry_number: int) -> datetime:
    """
    计算下次重试时间：指数退避 + ±10% jitter。

    公式：wait = min(2^(retry_number - 1), 720) 分钟
    retry_number 为 1-indexed 的重试序号（第一次重试 = 1）：

        retry 1  →  1  min
        retry 2  →  2  min
        retry 3  →  4  min
        retry 4  →  8  min
        retry 5  →  16 min
        retry 6  →  32 min
        retry 7  →  64 min  (~1h)
        retry 8  →  128 min (~2h)
        retry 9  →  256 min (~4h)
        retry 10 →  512 min (~8.5h)
        retry 11+→  720 min (12h, 上限)
    """
    wait_minutes = min(2 ** (retry_number - 1), 720)
    jitter = 1.0 + random.uniform(-0.1, 0.1)  # ±10%
    wait_seconds = wait_minutes * 60 * jitter
    return datetime.utcnow() + timedelta(seconds=wait_seconds)
