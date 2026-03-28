import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Protocol

from notification.database import dt_str, get_connection, row_to_job
from notification.models import NotificationJob

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Protocol（接口定义）
# ─────────────────────────────────────────────────────────────────────────────


class TaskQueue(Protocol):
    """
    任务队列接口。

    MVP 实现：SQLiteTaskQueue（DB 轮询，单线程）
    可替换实现：RedisTaskQueue、RabbitMQTaskQueue 等，调用方无需修改。
    """

    def poll(self, batch_size: int) -> list[NotificationJob]:
        """
        获取一批待投递任务，并将其标记为 processing。
        实现层须保证同一任务不会被并发获取（单线程天然满足，多线程需行锁）。
        """
        ...

    def ack(self, job_id: str) -> None:
        """标记任务投递成功（status → delivered）。"""
        ...

    def nack(self, job_id: str, retry_at: datetime, error: str) -> None:
        """
        标记任务投递失败，设置下次重试时间（status → failed）。
        attempt_count 加一，记录最近一次错误信息。
        """
        ...

    def dead(self, job_id: str, error: str) -> None:
        """
        标记任务超出最大重试次数，进入死信状态（status → dead）。
        attempt_count 加一，记录最终错误信息。
        """
        ...

    def recover_stale(self, stale_minutes: int) -> int:
        """
        启动恢复：将超时仍处于 processing 状态的任务重置为 pending。
        用于处理进程崩溃后遗留的滞留任务，返回恢复的任务数量。
        """
        ...


# ─────────────────────────────────────────────────────────────────────────────
# SQLite 实现
# ─────────────────────────────────────────────────────────────────────────────


class SQLiteTaskQueue(TaskQueue):
    """
    基于 notification_jobs 表的单线程任务队列实现。

    设计说明：
    - MVP 阶段 Dispatcher 单线程运行，无并发竞争，无需 SELECT FOR UPDATE SKIP LOCKED。
    - poll() 在同一事务中完成"查询 + 标记 processing"，保证原子性。
    - 若未来需要多 Worker 并发，可在 poll() 中加入行锁（目标 DB 支持时），
      或整体替换为 RedisTaskQueue / RabbitMQTaskQueue，调用方代码不变。
    - 每个方法独立获取连接，配合 SQLite WAL 模式保证 Gateway 与 Dispatcher
      并发读写时的安全性。
    """

    def __init__(self, db_path: str | None = None) -> None:
        self._db_path = db_path

    def _conn(self) -> sqlite3.Connection:
        return get_connection(self._db_path)

    # ── 核心操作 ──────────────────────────────────────────────────────────────

    def poll(self, batch_size: int = 10) -> list[NotificationJob]:
        """
        获取待投递任务并原子性地标记为 processing。

        查询条件：
          - status IN ('pending', 'failed')
          - next_retry_at <= 当前时间
        按 next_retry_at 升序排列，优先处理等待最久的任务。
        """
        now = dt_str(datetime.utcnow())
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM notification_jobs
                WHERE  status IN ('pending', 'failed')
                  AND  next_retry_at <= ?
                ORDER BY next_retry_at ASC
                LIMIT  ?
                """,
                (now, batch_size),
            ).fetchall()

            if not rows:
                return []

            jobs = [row_to_job(row) for row in rows]
            ids_placeholder = ",".join("?" * len(jobs))
            conn.execute(
                f"""
                UPDATE notification_jobs
                SET    status     = 'processing',
                       updated_at = ?
                WHERE  id IN ({ids_placeholder})
                """,
                [now, *[j.id for j in jobs]],
            )

        logger.debug("Polled %d job(s) from queue.", len(jobs))
        return jobs

    def ack(self, job_id: str) -> None:
        """投递成功，将任务标记为 delivered。"""
        now = dt_str(datetime.utcnow())
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE notification_jobs
                SET    status     = 'delivered',
                       updated_at = ?
                WHERE  id = ?
                """,
                (now, job_id),
            )
        logger.debug("Job %s acknowledged (delivered).", job_id)

    def nack(self, job_id: str, retry_at: datetime, error: str) -> None:
        """
        投递失败，将任务标记为 failed，设置下次重试时间。
        attempt_count 加一，记录最近错误。
        """
        now = dt_str(datetime.utcnow())
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE notification_jobs
                SET    status        = 'failed',
                       attempt_count = attempt_count + 1,
                       next_retry_at = ?,
                       last_error    = ?,
                       updated_at    = ?
                WHERE  id = ?
                """,
                (dt_str(retry_at), _truncate(error), now, job_id),
            )
        logger.debug(
            "Job %s nacked, retry at %s.", job_id, retry_at.isoformat()
        )

    def dead(self, job_id: str, error: str) -> None:
        """
        超出最大重试次数，将任务标记为 dead。
        attempt_count 加一，记录最终错误，并写日志告警（生产环境可替换为外部告警）。
        """
        now = dt_str(datetime.utcnow())
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE notification_jobs
                SET    status        = 'dead',
                       attempt_count = attempt_count + 1,
                       last_error    = ?,
                       updated_at    = ?
                WHERE  id = ?
                """,
                (_truncate(error), now, job_id),
            )
        # TODO: 触发外部告警（钉钉 / PagerDuty 等）
        logger.error(
            "Job %s entered DEAD state. Manual intervention required. Error: %s",
            job_id,
            _truncate(error, 300),
        )

    # ── 启动恢复 ──────────────────────────────────────────────────────────────

    def recover_stale(self, stale_minutes: int = 30) -> int:
        """
        启动恢复逻辑：将超时仍处于 processing 状态的任务重置为 pending。

        场景：Dispatcher 进程崩溃后，被标记为 processing 的任务不会自动回滚。
        系统启动时调用此方法，避免任务永久卡死。

        stale_minutes：updated_at 超过此时间仍为 processing 的任务视为滞留。
        """
        threshold = dt_str(datetime.utcnow() - timedelta(minutes=stale_minutes))
        now = dt_str(datetime.utcnow())
        with self._conn() as conn:
            result = conn.execute(
                """
                UPDATE notification_jobs
                SET    status     = 'pending',
                       updated_at = ?
                WHERE  status     = 'processing'
                  AND  updated_at <= ?
                """,
                (now, threshold),
            )
            count = result.rowcount

        if count:
            logger.warning(
                "Recovered %d stale 'processing' job(s) → 'pending'.", count
            )
        else:
            logger.debug("No stale jobs found during recovery.")

        return count


# ─────────────────────────────────────────────────────────────────────────────
# 辅助函数
# ─────────────────────────────────────────────────────────────────────────────


def _truncate(text: str, max_len: int = 1000) -> str:
    """截断过长的错误信息，避免占用过多存储空间。"""
    if len(text) <= max_len:
        return text
    return text[:max_len] + f"... [truncated, total {len(text)} chars]"
