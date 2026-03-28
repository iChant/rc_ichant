import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from notification.config import settings
from notification.database import init_db
from notification.dispatcher import Dispatcher
from notification.models import NotificationJob
from notification.queue import SQLiteTaskQueue
from notification.renderer import (
    build_template_context,
    render_body,
    render_headers,
)
from notification.repositories import (
    SQLiteCustomerRepository,
    SQLiteNotificationRepository,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# 应用生命周期
# ─────────────────────────────────────────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    """管理 Dispatcher 的启动与停止，与 FastAPI 应用生命周期绑定。"""
    init_db()

    queue = SQLiteTaskQueue(settings.db_path)
    dispatcher = Dispatcher(queue=queue)
    dispatcher.start()

    logger.info("Notification service started.")
    yield

    dispatcher.stop()
    logger.info("Notification service stopped.")


app = FastAPI(
    title="Notification Service",
    description="异步 HTTP 通知投递服务",
    version="0.1.0",
    lifespan=lifespan,
)


# ─────────────────────────────────────────────────────────────────────────────
# 依赖工厂
# ─────────────────────────────────────────────────────────────────────────────


def get_notification_repo() -> SQLiteNotificationRepository:
    return SQLiteNotificationRepository(settings.db_path)


def get_customer_repo() -> SQLiteCustomerRepository:
    return SQLiteCustomerRepository(settings.db_path)


# ─────────────────────────────────────────────────────────────────────────────
# 请求 / 响应 Schema
# ─────────────────────────────────────────────────────────────────────────────


class NotificationRequest(BaseModel):
    customer_id: str
    title: str | None = None
    content: dict[str, Any] | None = None
    idempotency_key: str | None = None
    event_type: str | None = None


# ─────────────────────────────────────────────────────────────────────────────
# 路由
# ─────────────────────────────────────────────────────────────────────────────


@app.post(
    "/api/v1/notifications",
    summary="提交通知请求",
    description=(
        "业务系统调用此接口提交通知请求。"
        "服务将查询客户 webhook 配置、渲染消息模板并持久化投递任务，"
        "立即返回 202 Accepted，异步完成投递。"
    ),
)
def submit_notification(body: NotificationRequest) -> JSONResponse:
    notification_repo = get_notification_repo()
    customer_repo = get_customer_repo()

    # ── 幂等检查 ──────────────────────────────────────────────────────────────
    if body.idempotency_key:
        existing = notification_repo.get_by_idempotency_key(
            body.idempotency_key
        )
        if existing:
            logger.info(
                "Idempotent request: returning existing job %s (status=%s).",
                existing.id,
                existing.status,
            )
            return JSONResponse(
                content={"job_id": existing.id, "status": existing.status},
                status_code=200,
            )

    # ── 查询客户 webhook 配置 ─────────────────────────────────────────────────
    config = customer_repo.get_webhook_config(body.customer_id)
    if config is None:
        raise HTTPException(
            status_code=400,
            detail=f"No webhook config found for customer: {body.customer_id!r}. "
            "Ensure the customer exists and has a webhook_url configured.",
        )

    # ── 渲染模板 ──────────────────────────────────────────────────────────────
    ctx = build_template_context(body.customer_id, body.title, body.content)
    try:
        headers_str = render_headers(config.headers_template, ctx)
        rendered_body = render_body(config.body_template, ctx)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    # ── 创建并持久化投递任务 ───────────────────────────────────────────────────
    now = datetime.utcnow()
    job = NotificationJob(
        id=str(uuid.uuid4()),
        target_url=config.endpoint_url,
        http_method=config.http_method,
        headers=headers_str,
        body=rendered_body or None,
        idempotency_key=body.idempotency_key,
        customer_id=body.customer_id,
        event_type=body.event_type,
        status="pending",
        attempt_count=0,
        max_attempts=config.max_retries,
        next_retry_at=now,
        last_error=None,
        created_at=now,
        updated_at=now,
    )
    notification_repo.create(job)

    logger.info(
        "Notification job %s created for customer %s (event=%s).",
        job.id,
        body.customer_id,
        body.event_type or "N/A",
    )

    return JSONResponse(
        content={"job_id": job.id, "status": "pending"},
        status_code=202,
    )


@app.get(
    "/api/v1/notifications/{job_id}",
    summary="查询投递状态",
    description="按 job_id 查询通知任务的投递状态，主要用于问题排查。",
)
def get_notification(job_id: str) -> JSONResponse:
    notification_repo = get_notification_repo()
    job = notification_repo.get_by_id(job_id)

    if job is None:
        raise HTTPException(
            status_code=404,
            detail=f"Job not found: {job_id!r}",
        )

    return JSONResponse(
        content={
            "job_id": job.id,
            "customer_id": job.customer_id,
            "event_type": job.event_type,
            "status": job.status,
            "attempt_count": job.attempt_count,
            "max_attempts": job.max_attempts,
            "next_retry_at": job.next_retry_at.isoformat(),
            "last_error": job.last_error,
            "created_at": job.created_at.isoformat(),
            "updated_at": job.updated_at.isoformat(),
        }
    )
