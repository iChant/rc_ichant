import logging
import sqlite3
from datetime import datetime

from notification.config import settings
from notification.models import NotificationJob

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# 通知系统表（属于通知系统，由通知系统创建和维护）
# ─────────────────────────────────────────────────────────────────────────────

NOTIFICATION_SYSTEM_DDL = """
CREATE TABLE IF NOT EXISTS notification_jobs (
    id               TEXT     PRIMARY KEY,
    -- Gateway 渲染模板后写入，Dispatcher 直接使用，不再访问用户表
    target_url       TEXT     NOT NULL,
    http_method      TEXT     NOT NULL DEFAULT 'POST',
    headers          TEXT     NOT NULL DEFAULT '{}',  -- JSON 字符串
    body             TEXT,

    -- 幂等与追踪
    idempotency_key  TEXT     UNIQUE,    -- NULL 不参与唯一约束（SQLite 行为）
    customer_id      TEXT     NOT NULL,  -- 保留原始 customer_id，便于审计
    event_type       TEXT,              -- 可选，仅用于日志追踪

    -- 状态与重试
    status           TEXT     NOT NULL DEFAULT 'pending',
                     -- pending | processing | delivered | failed | dead
    attempt_count    INTEGER  NOT NULL DEFAULT 0,
    max_attempts     INTEGER  NOT NULL DEFAULT 10,
    next_retry_at    DATETIME NOT NULL,
    last_error       TEXT,

    created_at       DATETIME NOT NULL,
    updated_at       DATETIME NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_poll
    ON notification_jobs (status, next_retry_at);
"""


# ─────────────────────────────────────────────────────────────────────────────
# 业务系统表（属于业务系统，由业务系统创建和维护）
#
# MVP 阶段与通知系统共享同一 SQLite 文件以简化部署。
# 生产环境中，CustomerRepository 的实现可切换为：
#   - 读取独立数据库（配置不同的连接字符串）
#   - 调用业务系统提供的 REST API
#
# 通知系统对这些表只有只读权限，不负责写入或维护。
# ─────────────────────────────────────────────────────────────────────────────

BUSINESS_SYSTEM_DDL = """
-- 客户表
-- 业务系统在客户注册时创建记录，并在注册流程中填写 webhook 配置字段。
-- 通知系统通过 customer_id 查询此表获取投递端点与模板信息。
CREATE TABLE IF NOT EXISTS customers (
    id    TEXT PRIMARY KEY,
    name  TEXT NOT NULL,
    email TEXT,

    -- ── Webhook 配置（由业务系统注册流程写入，通知系统只读） ──────────────
    --
    -- webhook_url：目标投递地址。NULL 表示该客户未配置 webhook，
    --   通知系统在接收请求时会返回 400。
    webhook_url          TEXT,

    -- webhook_method：HTTP 方法，通常为 POST。
    webhook_method       TEXT    NOT NULL DEFAULT 'POST',

    -- webhook_headers_tpl：Headers 的 Jinja2 模板，渲染后应为合法 JSON 对象。
    --   可用模板变量：{{ customer_id }}、{{ title }}、{{ content }}
    --   示例：{"Content-Type": "application/json", "X-Source": "{{ customer_id }}"}
    webhook_headers_tpl  TEXT    NOT NULL DEFAULT '{"Content-Type": "application/json"}',

    -- webhook_body_tpl：请求体的 Jinja2 模板。
    --   可用模板变量：{{ customer_id }}、{{ title }}、{{ content }}（dict）
    --   支持 Jinja2 的 tojson 过滤器将 Python 对象序列化为 JSON 字符串。
    --   示例：{"event": "{{ title }}", "data": {{ content | tojson }}}
    webhook_body_tpl     TEXT,

    -- webhook_timeout_s：单次 HTTP 请求的超时时间（秒）。
    webhook_timeout_s    INTEGER NOT NULL DEFAULT 30,

    -- webhook_max_retries：最大重试次数，超出后任务进入 dead 状态。
    webhook_max_retries  INTEGER NOT NULL DEFAULT 10,
    -- ─────────────────────────────────────────────────────────────────────

    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


# ─────────────────────────────────────────────────────────────────────────────
# 连接管理
# ─────────────────────────────────────────────────────────────────────────────


def get_connection(db_path: str | None = None) -> sqlite3.Connection:
    """
    创建并返回 SQLite 连接。

    开启 WAL 模式：允许多个读者与一个写者并发，适合 Gateway（读写）与
    Dispatcher（读写）同时运行的场景。
    每次调用返回新连接，调用方负责在 with 语句中管理事务。
    """
    path = db_path or settings.db_path
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: str | None = None) -> None:
    """初始化数据库，创建所有必要的表（幂等，可重复调用）。"""
    logger.info("Initializing database at '%s'...", db_path or settings.db_path)
    with get_connection(db_path) as conn:
        conn.executescript(NOTIFICATION_SYSTEM_DDL)
        conn.executescript(BUSINESS_SYSTEM_DDL)
    logger.info("Database initialized.")


# ─────────────────────────────────────────────────────────────────────────────
# 行转换辅助函数（供 repositories.py 和 queue.py 共用）
# ─────────────────────────────────────────────────────────────────────────────


def row_to_job(row: sqlite3.Row) -> NotificationJob:
    """将 notification_jobs 的一行数据转换为 NotificationJob 对象。"""
    return NotificationJob(
        id=row["id"],
        target_url=row["target_url"],
        http_method=row["http_method"],
        headers=row["headers"],
        body=row["body"],
        idempotency_key=row["idempotency_key"],
        customer_id=row["customer_id"],
        event_type=row["event_type"],
        status=row["status"],
        attempt_count=row["attempt_count"],
        max_attempts=row["max_attempts"],
        next_retry_at=datetime.fromisoformat(row["next_retry_at"]),
        last_error=row["last_error"],
        created_at=datetime.fromisoformat(row["created_at"]),
        updated_at=datetime.fromisoformat(row["updated_at"]),
    )


def dt_str(dt: datetime) -> str:
    """将 datetime 转换为 ISO 格式字符串，用于写入 SQLite DATETIME 字段。"""
    return dt.isoformat()
