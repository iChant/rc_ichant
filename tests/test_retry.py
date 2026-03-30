"""
tests/test_retry.py — 测试用例 2：重试降级机制

场景：
    模拟外部 Webhook 服务器临时不可用（前 N 次请求返回 503），
    验证通知模块的重试机制：
      - 投递失败后自动重试
      - 重试次数记录在 attempt_count 中
      - 服务恢复后最终投递成功（status = delivered）
      - 超出最大重试次数后任务进入 dead 状态

    为使测试快速完成，通过 monkeypatch 将重试延迟缩短为 1 秒，
    并将 Dispatcher 轮询间隔设置为 0.5 秒。

运行方式：
    uv run pytest tests/test_retry.py -v -s
"""

import json
from datetime import datetime, timedelta

import httpx

import notification.dispatcher as dispatcher_module
from notification.api import app
from notification.config import settings

from .helpers import (
    MockCustomerServer,
    NotificationUvicornServer,
    add_test_customer,
    find_free_port,
    wait_for_job_terminal,
)

# ─────────────────────────────────────────────────────────────────────────────
# 常量
# ─────────────────────────────────────────────────────────────────────────────

CUSTOMER_ID = "test_retry_customer"

# 快速重试延迟：1 秒（替代默认的分钟级指数退避）
_FAST_RETRY_DELAY_SECONDS = 1


def _fast_retry_delay(retry_number: int) -> datetime:
    """测试用重试延迟函数：固定 1 秒后重试，忽略 retry_number。"""
    return datetime.utcnow() + timedelta(seconds=_FAST_RETRY_DELAY_SECONDS)


# ─────────────────────────────────────────────────────────────────────────────
# 测试用例
# ─────────────────────────────────────────────────────────────────────────────


class TestRetryMechanism:
    """
    测试用例 2：重试降级机制

    验证通知模块在外部服务临时不可用时的重试行为。
    """

    def test_retry_until_success(self, temp_db, monkeypatch):
        """
        核心重试测试：mock 服务器前 3 次返回 503，第 4 次返回 200。

        验证：
        - 通知模块自动重试，最终投递成功（status = delivered）
        - attempt_count == FAIL_COUNT（记录了失败次数）
        - mock 服务器共收到 FAIL_COUNT + 1 次请求
        - 最终投递内容与原始消息一致
        """
        FAIL_COUNT = 3

        # ── Patch：隔离数据库 + 加速轮询 ──────────────────────────────────────
        monkeypatch.setattr(settings, "db_path", temp_db)
        monkeypatch.setattr(settings, "poll_interval_seconds", 0.5)

        # ── Patch：将重试延迟从「分钟级指数退避」替换为「1 秒固定延迟」──────────
        # _calc_retry_at 是 Dispatcher._handle_failure 调用的模块级函数。
        # monkeypatch.setattr 在测试结束后自动恢复原始函数。
        monkeypatch.setattr(
            dispatcher_module,
            "_calc_retry_at",
            _fast_retry_delay,
        )

        # ── 启动 mock 客户服务器（前 3 次返回 503）────────────────────────────
        mock_server = MockCustomerServer(fail_count=FAIL_COUNT)
        mock_server.start()
        print(
            f"\n[Test] Mock 服务器已启动：{mock_server.url}\n"
            f"       配置：前 {FAIL_COUNT} 次请求返回 503，之后返回 200"
        )

        # ── 写入测试客户（max_retries=5，大于 FAIL_COUNT）─────────────────────
        add_test_customer(
            db_path=temp_db,
            customer_id=CUSTOMER_ID,
            webhook_url=mock_server.url,
            max_retries=5,
        )

        # ── 启动通知模块服务器 ──────────────────────────────────────────────────
        notify_server = NotificationUvicornServer(app, port=find_free_port())
        notify_server.start()
        print(f"[Test] 通知模块服务器已启动：{notify_server.base_url}")

        try:
            # ── 提交通知请求 ────────────────────────────────────────────────────
            test_message = "hello, this is a testing message"
            resp = httpx.post(
                f"{notify_server.base_url}/api/v1/notifications",
                json={
                    "customer_id": CUSTOMER_ID,
                    "title": test_message,
                    "event_type": "retry_test",
                    "content": {
                        "fail_count": FAIL_COUNT,
                        "test_case": "test_retry_until_success",
                    },
                },
                timeout=5.0,
            )

            assert resp.status_code == 202, (
                f"Expected HTTP 202, got {resp.status_code}. Body: {resp.text}"
            )
            job_id = resp.json()["job_id"]
            print(
                f"[Test] 通知已提交，job_id: {job_id}\n"
                f"[Test] 等待 Dispatcher 重试..."
            )

            # ── 等待最终投递成功 ────────────────────────────────────────────────
            # 预期时间线（poll=0.5s，retry_delay=1s）：
            #   t≈0.5s : 第 1 次投递 → 503 → nack(retry_at=t+1s)
            #   t≈2.0s : 第 2 次投递 → 503 → nack(retry_at=t+1s)
            #   t≈3.5s : 第 3 次投递 → 503 → nack(retry_at=t+1s)
            #   t≈5.0s : 第 4 次投递 → 200 → ack → delivered
            timeout = (FAIL_COUNT + 2) * 3.0
            result = wait_for_job_terminal(
                notify_server.base_url, job_id, timeout=timeout
            )

            # ── 验证最终状态 ────────────────────────────────────────────────────
            assert result["status"] == "delivered", (
                f"Expected status='delivered', got '{result['status']}'. "
                f"Full result: {result}"
            )

            # attempt_count 记录的是失败次数（nack 每次 +1，ack 不递增）
            assert result["attempt_count"] == FAIL_COUNT, (
                f"Expected attempt_count={FAIL_COUNT} (one per failure), "
                f"got {result['attempt_count']}"
            )

            # mock 服务器应收到 FAIL_COUNT + 1 次请求（失败 + 最终成功）
            assert mock_server.request_count == FAIL_COUNT + 1, (
                f"Mock server should have received {FAIL_COUNT + 1} requests "
                f"({FAIL_COUNT} failures + 1 success), "
                f"got {mock_server.request_count}"
            )

            # 最后一次收到的内容应与原始消息一致
            received = mock_server.last_payload
            assert received is not None
            assert received.get("message") == test_message, (
                f"Expected message={test_message!r}, "
                f"got {received.get('message')!r}"
            )

            print(
                f"\n[Test] ✅ 重试机制验证通过\n"
                f"         总请求次数（mock 收到）: {mock_server.request_count}\n"
                f"         失败次数: {FAIL_COUNT}\n"
                f"         最终成功: 1\n"
                f"         attempt_count（DB 记录）: {result['attempt_count']}\n"
                f"         最终投递内容:\n"
                f"         {json.dumps(received, ensure_ascii=False, indent=10)}"
            )

        finally:
            notify_server.stop()
            mock_server.stop()

    def test_dead_letter_on_max_retries_exceeded(self, temp_db, monkeypatch):
        """
        死信测试：mock 服务器持续返回 503，超出最大重试次数后，
        任务应进入 dead 状态，不再重试。

        验证：
        - 任务最终 status = dead
        - attempt_count == max_retries（恰好用完所有重试机会）
        - mock 服务器收到的请求数 == max_retries
        """
        MAX_RETRIES = 3  # 故意设置较小值以加速测试

        monkeypatch.setattr(settings, "db_path", temp_db)
        monkeypatch.setattr(settings, "poll_interval_seconds", 0.5)
        monkeypatch.setattr(
            dispatcher_module,
            "_calc_retry_at",
            _fast_retry_delay,
        )

        # mock 服务器始终返回 503（fail_count 远大于 max_retries）
        mock_server = MockCustomerServer(fail_count=MAX_RETRIES + 10)
        mock_server.start()
        print(
            f"\n[Test] Mock 服务器已启动（持续返回 503）：{mock_server.url}\n"
            f"       max_retries={MAX_RETRIES}，预期任务进入 dead 状态"
        )

        add_test_customer(
            db_path=temp_db,
            customer_id=CUSTOMER_ID,
            webhook_url=mock_server.url,
            max_retries=MAX_RETRIES,
        )

        notify_server = NotificationUvicornServer(app, port=find_free_port())
        notify_server.start()

        try:
            resp = httpx.post(
                f"{notify_server.base_url}/api/v1/notifications",
                json={
                    "customer_id": CUSTOMER_ID,
                    "title": "dead letter test",
                    "event_type": "dead_letter_test",
                    "content": {"max_retries": MAX_RETRIES},
                },
                timeout=5.0,
            )

            assert resp.status_code == 202
            job_id = resp.json()["job_id"]
            print(
                f"[Test] 通知已提交，job_id: {job_id}，等待任务进入 dead 状态..."
            )

            # 等待任务进入 dead 状态
            # 预期时间：MAX_RETRIES 次失败 × ~1.5s/次 + buffer
            timeout = (MAX_RETRIES + 2) * 3.0
            result = wait_for_job_terminal(
                notify_server.base_url, job_id, timeout=timeout
            )

            assert result["status"] == "dead", (
                f"Expected status='dead', got '{result['status']}'. "
                f"Full result: {result}"
            )

            # attempt_count 应等于 max_retries
            # 最后一次失败时 _handle_failure 调用 dead()，dead() 也会 +1
            assert result["attempt_count"] == MAX_RETRIES, (
                f"Expected attempt_count={MAX_RETRIES}, "
                f"got {result['attempt_count']}"
            )

            print(
                f"\n[Test] ✅ 死信机制验证通过\n"
                f"         任务状态: {result['status']}\n"
                f"         尝试次数: {result['attempt_count']}/{MAX_RETRIES}\n"
                f"         最后错误: {result.get('last_error', 'N/A')}"
            )

        finally:
            notify_server.stop()
            mock_server.stop()

    def test_retry_preserves_original_payload(self, temp_db, monkeypatch):
        """
        幂等投递内容测试：重试时每次投递的请求体应与初次完全一致。

        通知系统在 Gateway 接收时完成模板渲染，将结果存入 DB；
        Dispatcher 重试时直接使用 DB 中存储的内容，不重新渲染。
        验证所有重试请求的 body 与最终成功请求的 body 相同。
        """
        FAIL_COUNT = 2

        monkeypatch.setattr(settings, "db_path", temp_db)
        monkeypatch.setattr(settings, "poll_interval_seconds", 0.5)
        monkeypatch.setattr(
            dispatcher_module,
            "_calc_retry_at",
            _fast_retry_delay,
        )

        mock_server = MockCustomerServer(fail_count=FAIL_COUNT)
        mock_server.start()

        add_test_customer(
            db_path=temp_db,
            customer_id=CUSTOMER_ID,
            webhook_url=mock_server.url,
            max_retries=5,
        )

        notify_server = NotificationUvicornServer(app, port=find_free_port())
        notify_server.start()

        try:
            original_title = "payload consistency test"
            original_content = {"key": "value", "number": 42}

            resp = httpx.post(
                f"{notify_server.base_url}/api/v1/notifications",
                json={
                    "customer_id": CUSTOMER_ID,
                    "title": original_title,
                    "content": original_content,
                },
                timeout=5.0,
            )

            assert resp.status_code == 202
            job_id = resp.json()["job_id"]

            timeout = (FAIL_COUNT + 2) * 3.0
            result = wait_for_job_terminal(
                notify_server.base_url, job_id, timeout=timeout
            )

            assert result["status"] == "delivered"
            assert mock_server.request_count == FAIL_COUNT + 1

            # 解析所有收到的请求体
            all_payloads = [
                json.loads(raw) for raw in mock_server.received_payloads
            ]

            # 验证每次请求（含失败的）的内容都相同
            for i, payload in enumerate(all_payloads):
                assert payload.get("message") == original_title, (
                    f"Request #{i + 1}: expected message={original_title!r}, "
                    f"got {payload.get('message')!r}"
                )
                assert payload.get("content") == original_content, (
                    f"Request #{i + 1}: content mismatch. "
                    f"Expected {original_content}, got {payload.get('content')}"
                )

            print(
                f"\n[Test] ✅ 投递内容一致性验证通过\n"
                f"         共 {mock_server.request_count} 次请求（{FAIL_COUNT} 次失败 + 1 次成功）\n"
                f"         所有请求 body 完全一致"
            )

        finally:
            notify_server.stop()
            mock_server.stop()
