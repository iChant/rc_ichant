import logging
import sqlite3
from typing import Optional, Protocol

from notification.database import dt_str, get_connection, row_to_job
from notification.models import CustomerWebhookConfig, NotificationJob

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Protocols（接口定义）
# 上层逻辑（Gateway、Dispatcher）依赖这些接口，不依赖具体实现。
# 替换底层存储时只需提供新的实现类，无需修改调用方。
# ─────────────────────────────────────────────────────────────────────────────


class NotificationRepository(Protocol):
    """通知任务的持久化接口。"""

    def create(self, job: NotificationJob) -> str:
        """持久化一个新任务，返回 job_id。"""
        ...

    def get_by_id(self, job_id: str) -> Optional[NotificationJob]:
        """按 ID 查询任务，不存在时返回 None。"""
        ...

    def get_by_idempotency_key(self, key: str) -> Optional[NotificationJob]:
        """按幂等键查询任务，不存在时返回 None。"""
        ...


class CustomerRepository(Protocol):
    """用户 Webhook 配置的访问接口，屏蔽底层数据源（直连 DB 或外部 API）。"""

    def get_webhook_config(
        self, customer_id: str
    ) -> Optional[CustomerWebhookConfig]:
        """
        返回指定客户的 Webhook 配置。
        若客户不存在或未配置 webhook_url，返回 None。
        """
        ...


# ─────────────────────────────────────────────────────────────────────────────
# SQLite 实现
# ─────────────────────────────────────────────────────────────────────────────


class SQLiteNotificationRepository:
    """
    基于 SQLite 的通知任务持久化实现。

    每个方法独立创建连接，通过 WAL 模式保证 Gateway（FastAPI 线程池）
    与 Dispatcher（后台线程）并发读写时的安全性。
    """

    def __init__(self, db_path: str | None = None) -> None:
        self._db_path = db_path

    def _conn(self) -> sqlite3.Connection:
        return get_connection(self._db_path)

    def create(self, job: NotificationJob) -> str:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO notification_jobs (
                    id, target_url, http_method, headers, body,
                    idempotency_key, customer_id, event_type,
                    status, attempt_count, max_attempts, next_retry_at,
                    last_error, created_at, updated_at
                ) VALUES (
                    ?, ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?
                )
                """,
                (
                    job.id,
                    job.target_url,
                    job.http_method,
                    job.headers,
                    job.body,
                    job.idempotency_key,
                    job.customer_id,
                    job.event_type,
                    job.status,
                    job.attempt_count,
                    job.max_attempts,
                    dt_str(job.next_retry_at),
                    job.last_error,
                    dt_str(job.created_at),
                    dt_str(job.updated_at),
                ),
            )
        logger.debug("Created notification job %s.", job.id)
        return job.id

    def get_by_id(self, job_id: str) -> Optional[NotificationJob]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM notification_jobs WHERE id = ?",
                (job_id,),
            ).fetchone()
        return row_to_job(row) if row else None

    def get_by_idempotency_key(self, key: str) -> Optional[NotificationJob]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM notification_jobs WHERE idempotency_key = ?",
                (key,),
            ).fetchone()
        return row_to_job(row) if row else None


class SQLiteCustomerRepository:
    """
    读取业务系统 customers 表，获取客户的 Webhook 配置。

    MVP 阶段假设通知系统与业务系统共享同一 SQLite 文件，因此直接查询。
    生产环境中可替换为调用业务系统 REST API 的实现，接口不变。
    """

    def __init__(self, db_path: str | None = None) -> None:
        self._db_path = db_path

    def _conn(self) -> sqlite3.Connection:
        return get_connection(self._db_path)

    def get_webhook_config(
        self, customer_id: str
    ) -> Optional[CustomerWebhookConfig]:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT
                    webhook_url,
                    webhook_method,
                    webhook_headers_tpl,
                    webhook_body_tpl,
                    webhook_timeout_s,
                    webhook_max_retries
                FROM customers
                WHERE id = ?
                  AND webhook_url IS NOT NULL
                  AND webhook_url != ''
                """,
                (customer_id,),
            ).fetchone()

        if not row:
            logger.debug(
                "No webhook config found for customer '%s'.", customer_id
            )
            return None

        return CustomerWebhookConfig(
            endpoint_url=row["webhook_url"],
            http_method=row["webhook_method"],
            headers_template=row["webhook_headers_tpl"],
            body_template=row["webhook_body_tpl"] or "",
            timeout_seconds=row["webhook_timeout_s"],
            max_retries=row["webhook_max_retries"],
        )
