from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class NotificationJob:
    """通知投递任务，存储 Gateway 组装完毕的完整 HTTP 请求快照。"""

    id: str
    # 已组装的投递目标（由 Gateway 渲染模板后写入，Dispatcher 直接使用）
    target_url: str
    http_method: str
    headers: str  # JSON 字符串，兼容各数据库
    body: Optional[str]

    # 幂等与追踪
    idempotency_key: Optional[str]
    customer_id: str
    event_type: Optional[str]

    # 状态与重试
    status: str  # pending | processing | delivered | failed | dead
    attempt_count: int
    max_attempts: int
    next_retry_at: datetime
    last_error: Optional[str]

    created_at: datetime
    updated_at: datetime


@dataclass
class CustomerWebhookConfig:
    """从用户表读取的客户 Webhook 配置。"""

    endpoint_url: str
    http_method: str
    headers_template: str  # Jinja2 模板，渲染后应为合法 JSON 对象字符串
    body_template: str  # Jinja2 模板，渲染后为请求体字符串
    timeout_seconds: int
    max_retries: int
