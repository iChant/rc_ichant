"""
tests/test_delivery.py — 测试用例 1：端到端消息投递

场景：
    1. 启动一个 mock 客户服务器，监听 Webhook 投递请求，打印收到的内容
    2. 启动通知模块服务器（使用独立的临时 SQLite 数据库）
    3. 通过通知模块服务器，向 mock 客户服务器投递一条测试消息
    4. 验证：
       - 通知请求被接受（HTTP 202）
       - mock 服务器收到了投递内容且内容正确
       - 任务状态变为 delivered
       - 相同 idempotency_key 的重复提交返回 HTTP 200（幂等）

运行方式：
    uv run pytest tests/test_delivery.py -v -s
"""

import json

import httpx

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

CUSTOMER_ID = "test_delivery_customer"
IDEMPOTENCY_KEY = "test-delivery-idem-key-001"
TEST_MESSAGE = "hello, this is a testing message"


# ─────────────────────────────────────────────────────────────────────────────
# 测试用例
# ─────────────────────────────────────────────────────────────────────────────


class TestEndToEndDelivery:
    """
    测试用例 1：端到端消息投递

    验证通知模块从接收请求到最终将消息投递到外部 Webhook 的完整链路。
    """

    def test_message_delivered_to_mock_server(self, temp_db, monkeypatch):
        """
        主测试：提交一条通知，验证 mock 服务器收到内容正确的投递。

        流程：
            业务系统 → POST /api/v1/notifications（通知模块）
                      → [Dispatcher 异步投递]
                      → POST /webhook（mock 客户服务器）
        """
        # ── 1. Patch 运行时配置（隔离数据库 + 加速轮询）────────────────────────
        monkeypatch.setattr(settings, "db_path", temp_db)
        monkeypatch.setattr(settings, "poll_interval_seconds", 0.5)

        # ── 2. 启动 mock 客户服务器 ────────────────────────────────────────────
        mock_server = MockCustomerServer()
        mock_server.start()
        print(f"\n[Test] Mock 客户服务器已启动：{mock_server.url}")

        # ── 3. 向测试数据库写入客户记录（模拟业务系统注册时写入 webhook 配置）──
        add_test_customer(
            db_path=temp_db,
            customer_id=CUSTOMER_ID,
            webhook_url=mock_server.url,
        )

        # ── 4. 启动通知模块服务器 ──────────────────────────────────────────────
        notify_server = NotificationUvicornServer(app, port=find_free_port())
        notify_server.start()
        print(f"[Test] 通知模块服务器已启动：{notify_server.base_url}")

        try:
            # ── 5. 业务系统提交通知请求 ────────────────────────────────────────
            print(f'\n[Test] 提交通知："{TEST_MESSAGE}"')

            resp = httpx.post(
                f"{notify_server.base_url}/api/v1/notifications",
                json={
                    "customer_id": CUSTOMER_ID,
                    "title": TEST_MESSAGE,
                    "event_type": "integration_test",
                    "idempotency_key": IDEMPOTENCY_KEY,
                    "content": {
                        "source": "pytest",
                        "test_case": "test_message_delivered_to_mock_server",
                    },
                },
                timeout=5.0,
            )

            # ── 6. 验证通知被接受（202 Accepted）──────────────────────────────
            assert resp.status_code == 202, (
                f"Expected HTTP 202, got {resp.status_code}. Body: {resp.text}"
            )
            job_id = resp.json()["job_id"]
            assert job_id, "Response should contain a job_id"
            print(f"[Test] 通知已接受，job_id: {job_id}")

            # ── 7. 等待 Dispatcher 完成投递（轮询任务状态）────────────────────
            print("[Test] 等待 Dispatcher 投递...")
            result = wait_for_job_terminal(
                notify_server.base_url, job_id, timeout=15.0
            )

            # ── 8. 验证任务状态为 delivered ────────────────────────────────────
            assert result["status"] == "delivered", (
                f"Expected status='delivered', got: {result}"
            )
            assert result["attempt_count"] == 0, (
                "Message should be delivered on the first attempt "
                f"(attempt_count={result['attempt_count']})"
            )
            print(f"[Test] 任务状态确认：{result['status']}")

            # ── 9. 验证 mock 服务器收到了投递请求 ─────────────────────────────
            assert mock_server.request_count >= 1, (
                "Mock server should have received at least 1 request"
            )

            # ── 10. 验证投递内容正确 ───────────────────────────────────────────
            received = mock_server.last_payload
            assert received is not None, (
                "Mock server should have a received payload"
            )

            # body_template: {"message": "{{ title }}", "content": ..., "customer_id": ...}
            assert received.get("message") == TEST_MESSAGE, (
                f"Expected message={TEST_MESSAGE!r}, got: {received.get('message')!r}"
            )
            assert received.get("customer_id") == CUSTOMER_ID
            assert received.get("content", {}).get("source") == "pytest"

            print(
                f"\n[Test] ✅ 端到端投递验证通过\n"
                f"         Mock 服务器收到请求数: {mock_server.request_count}\n"
                f"         投递内容:\n"
                f"         {json.dumps(received, ensure_ascii=False, indent=10)}"
            )

        finally:
            notify_server.stop()
            mock_server.stop()

    def test_idempotent_resubmission(self, temp_db, monkeypatch):
        """
        幂等测试：携带相同 idempotency_key 重复提交同一通知，
        应返回 HTTP 200 并复用已有任务，不创建新任务。

        这模拟了业务系统因网络超时等原因重试提交的场景。
        """
        monkeypatch.setattr(settings, "db_path", temp_db)
        monkeypatch.setattr(settings, "poll_interval_seconds", 0.5)

        mock_server = MockCustomerServer()
        mock_server.start()

        add_test_customer(
            db_path=temp_db,
            customer_id=CUSTOMER_ID,
            webhook_url=mock_server.url,
        )

        notify_server = NotificationUvicornServer(app, port=find_free_port())
        notify_server.start()

        idem_key = "test-idempotency-unique-key-999"
        notification_payload = {
            "customer_id": CUSTOMER_ID,
            "title": "idempotency test message",
            "idempotency_key": idem_key,
            "content": {"attempt": 1},
        }

        try:
            # 第一次提交 → 应返回 202（新任务）
            resp1 = httpx.post(
                f"{notify_server.base_url}/api/v1/notifications",
                json=notification_payload,
                timeout=5.0,
            )
            assert resp1.status_code == 202, (
                f"First submission should return 202, got {resp1.status_code}"
            )
            job_id_first = resp1.json()["job_id"]

            # 第二次提交（相同 idempotency_key）→ 应返回 200（复用已有任务）
            resp2 = httpx.post(
                f"{notify_server.base_url}/api/v1/notifications",
                json={**notification_payload, "content": {"attempt": 2}},
                timeout=5.0,
            )
            assert resp2.status_code == 200, (
                f"Duplicate submission should return 200, got {resp2.status_code}"
            )
            job_id_second = resp2.json()["job_id"]

            # 两次提交应该返回相同的 job_id
            assert job_id_first == job_id_second, (
                f"Idempotent requests must return the same job_id. "
                f"First: {job_id_first}, Second: {job_id_second}"
            )

            # 等待投递完成，确保最终只投递了一次
            wait_for_job_terminal(
                notify_server.base_url, job_id_first, timeout=12.0
            )
            assert mock_server.request_count == 1, (
                f"Idempotent resubmission should result in exactly 1 delivery, "
                f"got {mock_server.request_count}"
            )

            print(
                f"\n[Test] ✅ 幂等验证通过\n"
                f"         两次提交均返回 job_id: {job_id_first}\n"
                f"         Mock 服务器仅收到 1 次投递（无重复）"
            )

        finally:
            notify_server.stop()
            mock_server.stop()

    def test_unknown_customer_returns_400(self, temp_db, monkeypatch):
        """
        边界测试：提交对象为不存在的 customer_id，
        Gateway 应立即返回 HTTP 400，不创建任何投递任务。
        """
        monkeypatch.setattr(settings, "db_path", temp_db)
        monkeypatch.setattr(settings, "poll_interval_seconds", 0.5)

        # 初始化空数据库（不插入任何客户）
        from notification.database import init_db

        init_db(temp_db)

        notify_server = NotificationUvicornServer(app, port=find_free_port())
        notify_server.start()

        try:
            resp = httpx.post(
                f"{notify_server.base_url}/api/v1/notifications",
                json={
                    "customer_id": "non_existent_customer_xyz",
                    "title": "test",
                    "content": {},
                },
                timeout=5.0,
            )
            assert resp.status_code == 400, (
                f"Unknown customer should return 400, got {resp.status_code}"
            )
            error_detail = resp.json().get("detail", "")
            assert "non_existent_customer_xyz" in error_detail, (
                f"Error message should mention the customer_id. Got: {error_detail}"
            )
            print(f"\n[Test] ✅ 未知客户验证通过，返回 400: {error_detail}")
        finally:
            notify_server.stop()
