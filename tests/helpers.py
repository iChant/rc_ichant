"""
tests/helpers.py — 集成测试共享工具

提供：
  - find_free_port()          : 获取系统可用的 TCP 端口
  - MockCustomerServer        : 模拟外部客户 Webhook 服务器（可配置失败次数）
  - NotificationUvicornServer : 在后台线程中运行通知系统 FastAPI 应用
  - add_test_customer()       : 向测试数据库写入样本客户及 Webhook 配置
  - wait_for_job_terminal()   : 轮询通知服务器，等待任务到达终止状态
"""

import json
import socket
import threading
import time

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import Response

# ─────────────────────────────────────────────────────────────────────────────
# 端口工具
# ─────────────────────────────────────────────────────────────────────────────


def find_free_port() -> int:
    """绑定到系统分配的随机端口，立即释放，返回该端口号。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


# ─────────────────────────────────────────────────────────────────────────────
# Mock 客户服务器
# ─────────────────────────────────────────────────────────────────────────────


class MockCustomerServer:
    """
    模拟外部客户 Webhook 服务器。

    功能：
    - 监听 POST /webhook，记录所有收到的请求体
    - 可配置前 N 次请求返回 503（模拟服务临时不可用）
    - 成功收到投递后，将 JSON 内容打印到 stdout（测试输出可见）

    线程安全：所有对内部状态的读写均通过 threading.Lock 保护。

    典型用法：
        server = MockCustomerServer(fail_count=3)
        server.start()
        # ... 运行测试 ...
        server.stop()
        assert server.request_count == 4  # 3 次失败 + 1 次成功
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int | None = None,
        fail_count: int = 0,
    ) -> None:
        self.host = host
        self.port = port or find_free_port()
        self._fail_remaining = fail_count
        self._received_payloads: list[str] = []  # 所有请求体（含失败请求）
        self._lock = threading.Lock()

        self._app = self._build_app()
        config = uvicorn.Config(
            self._app,
            host=self.host,
            port=self.port,
            log_level="error",
        )
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(
            target=self._server.run,
            name="mock-customer-server",
            daemon=True,
        )

    def _build_app(self) -> FastAPI:
        app = FastAPI()

        @app.post("/webhook")
        async def receive_notification(request: Request) -> Response:
            body_bytes = await request.body()
            payload_str = body_bytes.decode("utf-8")

            with self._lock:
                self._received_payloads.append(payload_str)
                should_fail = self._fail_remaining > 0
                if should_fail:
                    self._fail_remaining -= 1
                    remaining = self._fail_remaining

            if should_fail:
                print(
                    f"\n[MockServer] ❌ 返回 503（剩余失败次数: {remaining}）",
                    flush=True,
                )
                return Response(content="Service Unavailable", status_code=503)

            # 成功：打印收到的内容
            try:
                pretty = json.dumps(
                    json.loads(payload_str), ensure_ascii=False, indent=2
                )
            except (json.JSONDecodeError, ValueError):
                pretty = payload_str

            print(f"\n[MockServer] ✅ 收到通知投递：\n{pretty}\n", flush=True)
            return Response(
                content=json.dumps({"status": "ok"}),
                status_code=200,
                media_type="application/json",
            )

        return app

    # ── 属性 ──────────────────────────────────────────────────────────────────

    @property
    def url(self) -> str:
        """Webhook 端点完整 URL。"""
        return f"http://{self.host}:{self.port}/webhook"

    @property
    def request_count(self) -> int:
        """收到的请求总数（含失败请求）。"""
        with self._lock:
            return len(self._received_payloads)

    @property
    def received_payloads(self) -> list[str]:
        """所有收到的请求体字符串列表（按接收顺序）。"""
        with self._lock:
            return list(self._received_payloads)

    @property
    def last_payload(self) -> dict | None:
        """最后一次收到的请求体（解析为 dict）；无请求时返回 None。"""
        with self._lock:
            if not self._received_payloads:
                return None
            raw = self._received_payloads[-1]
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            return {"_raw": raw}

    # ── 生命周期 ──────────────────────────────────────────────────────────────

    def start(self) -> None:
        """启动服务器，阻塞直到可接受连接。"""
        self._thread.start()
        _wait_until_ready(
            f"http://{self.host}:{self.port}", check_path="/webhook"
        )

    def stop(self) -> None:
        """停止服务器。"""
        self._server.should_exit = True
        self._thread.join(timeout=3)


# ─────────────────────────────────────────────────────────────────────────────
# 通知服务器（后台线程）
# ─────────────────────────────────────────────────────────────────────────────


class NotificationUvicornServer:
    """
    在后台线程中运行通知系统 FastAPI 应用。

    设计说明：
    - 接受任意 FastAPI app 实例，通过调用方提前 patch settings 来隔离测试数据
    - 启动时阻塞，直到服务器可接受 HTTP 请求
    - 对外暴露 base_url 供测试代码调用

    典型用法：
        # 先 patch settings，再启动
        monkeypatch.setattr(settings, "db_path", temp_db)
        monkeypatch.setattr(settings, "poll_interval_seconds", 0.5)

        from notification.api import app
        server = NotificationUvicornServer(app)
        server.start()
        # ... 运行测试 ...
        server.stop()
    """

    def __init__(
        self,
        app: FastAPI,
        host: str = "127.0.0.1",
        port: int | None = None,
    ) -> None:
        self.host = host
        self.port = port or find_free_port()
        self.base_url = f"http://{self.host}:{self.port}"

        config = uvicorn.Config(
            app,
            host=self.host,
            port=self.port,
            log_level="warning",
        )
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(
            target=self._server.run,
            name="notification-server",
            daemon=True,
        )

    def start(self) -> None:
        """启动服务器，阻塞直到可接受连接。"""
        self._thread.start()
        _wait_until_ready(self.base_url, check_path="/docs")

    def stop(self) -> None:
        """停止服务器，最多等待 5 秒。"""
        self._server.should_exit = True
        self._thread.join(timeout=5)


# ─────────────────────────────────────────────────────────────────────────────
# 数据库辅助
# ─────────────────────────────────────────────────────────────────────────────

# 默认 Body 模板：将 title 映射为 message，content 整体内嵌为 JSON 对象
_DEFAULT_BODY_TEMPLATE = (
    '{"message": "{{ title }}", '
    '"content": {{ content | tojson }}, '
    '"customer_id": "{{ customer_id }}"}'
)

_DEFAULT_HEADERS_TEMPLATE = '{"Content-Type": "application/json"}'


def add_test_customer(
    db_path: str,
    customer_id: str,
    webhook_url: str,
    max_retries: int = 5,
    body_template: str = _DEFAULT_BODY_TEMPLATE,
    headers_template: str = _DEFAULT_HEADERS_TEMPLATE,
) -> None:
    """
    向测试数据库写入一条客户记录（模拟业务系统注册流程的行为）。

    使用 INSERT OR REPLACE，可重复调用（幂等）。
    """
    from notification.database import get_connection, init_db

    init_db(db_path)

    with get_connection(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO customers (
                id, name, webhook_url, webhook_method,
                webhook_headers_tpl, webhook_body_tpl,
                webhook_timeout_s, webhook_max_retries
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                customer_id,
                f"Test Customer [{customer_id}]",
                webhook_url,
                "POST",
                headers_template,
                body_template,
                30,
                max_retries,
            ),
        )


# ─────────────────────────────────────────────────────────────────────────────
# 等待辅助
# ─────────────────────────────────────────────────────────────────────────────


def _wait_until_ready(
    base_url: str,
    check_path: str = "/docs",
    timeout: float = 10.0,
) -> None:
    """
    轮询 base_url + check_path，直到服务器返回任意响应（含 4xx/5xx）。
    超时后抛出 RuntimeError。
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            httpx.get(f"{base_url}{check_path}", timeout=0.5)
            return
        except Exception:
            time.sleep(0.2)
    raise RuntimeError(
        f"Server at {base_url} did not become ready within {timeout}s"
    )


def wait_for_job_terminal(
    base_url: str,
    job_id: str,
    timeout: float = 15.0,
    poll_interval: float = 0.3,
) -> dict:
    """
    轮询通知服务器的 GET /api/v1/notifications/{job_id}，
    直到任务状态达到终止状态（delivered 或 dead）。

    返回最终的任务状态 dict；超时则抛出 TimeoutError。

    注意：
    - 'failed' 不是终止状态（它会被重新排队重试）
    - 'processing' 表示 Dispatcher 正在处理，继续等待
    """
    deadline = time.time() + timeout
    last_data: dict = {}
    while time.time() < deadline:
        try:
            resp = httpx.get(
                f"{base_url}/api/v1/notifications/{job_id}",
                timeout=5.0,
            )
            last_data = resp.json()
            status = last_data.get("status", "")
            if status in ("delivered", "dead"):
                return last_data
        except Exception:
            pass
        time.sleep(poll_interval)

    raise TimeoutError(
        f"Job {job_id!r} did not reach terminal state within {timeout}s. "
        f"Last response: {last_data}"
    )
