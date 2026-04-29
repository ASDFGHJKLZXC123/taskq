from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Any, AsyncIterator, Optional
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from taskq.broker import Broker
from taskq.db import create_pool
from taskq.models import Task
from taskq.settings import Settings

logger = logging.getLogger(__name__)


def _serialize_task(task: Task) -> dict[str, Any]:
    data = asdict(task)
    if isinstance(data.get("lease_id"), UUID):
        data["lease_id"] = str(data["lease_id"])
    for key in ("available_at", "created_at", "updated_at", "completed_at", "visible_until"):
        value = data.get(key)
        if isinstance(value, datetime):
            data[key] = value.isoformat()
    return data


class RequeueBody(BaseModel):
    payload: Optional[dict] = None
    max_attempts: Optional[int] = None
    delay_s: Optional[int] = None


async def _reap_loop(broker: Broker, settings: Settings) -> None:
    older_than = timedelta(days=settings.TASKQ_REAP_RETENTION_DAYS)
    interval = settings.TASKQ_REAP_INTERVAL_S
    while True:
        try:
            await broker.reap_completed(older_than)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception("reap_completed failed")
        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            raise


def create_app(broker: Optional[Broker] = None, settings: Optional[Settings] = None) -> FastAPI:
    settings = settings or Settings()

    if settings.TASKQ_ENV == "prod" and not settings.TASKQ_ADMIN_TOKEN:
        raise RuntimeError("admin token required in prod")

    if settings.TASKQ_ENV == "dev" and not settings.TASKQ_ADMIN_TOKEN:
        logger.warning(
            "TASKQ_ADMIN_TOKEN not set in dev; admin endpoints are unauthenticated"
        )

    bearer_scheme = HTTPBearer(auto_error=False)

    async def require_auth(
        creds: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
    ) -> None:
        token = settings.TASKQ_ADMIN_TOKEN
        if not token:
            return
        if creds is None or creds.scheme.lower() != "bearer" or creds.credentials != token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="invalid or missing bearer token",
            )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        owns_pool = False
        if broker is not None:
            app.state.broker = broker
            app.state.pool = None
        else:
            pool = await create_pool(settings)
            app.state.pool = pool
            app.state.broker = Broker(pool, settings)
            owns_pool = True

        reap_task: Optional[asyncio.Task] = None
        if settings.TASKQ_REAP_ENABLED:
            reap_task = asyncio.create_task(_reap_loop(app.state.broker, settings))
        app.state.reap_task = reap_task

        try:
            yield
        finally:
            if reap_task is not None:
                reap_task.cancel()
                await asyncio.gather(reap_task, return_exceptions=True)
            if owns_pool and app.state.pool is not None:
                await app.state.pool.close()

    app = FastAPI(lifespan=lifespan)

    def _get_broker(request: Request) -> Broker:
        return request.app.state.broker

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/queues/{queue}/stats", dependencies=[Depends(require_auth)])
    async def queue_stats(queue: str, request: Request) -> dict[str, Any]:
        b = _get_broker(request)
        return await b.stats(queue)

    @app.get("/queues/{queue}/dead", dependencies=[Depends(require_auth)])
    async def queue_dead(
        queue: str,
        request: Request,
        limit: int = 100,
        before_id: Optional[int] = None,
    ) -> dict[str, list[dict[str, Any]]]:
        b = _get_broker(request)
        tasks = await b.list_dead(queue, limit=limit, before_id=before_id)
        return {"tasks": [_serialize_task(t) for t in tasks]}

    @app.post("/tasks/{task_id}/requeue", dependencies=[Depends(require_auth)])
    async def task_requeue(
        task_id: int, body: RequeueBody, request: Request
    ) -> dict[str, bool]:
        b = _get_broker(request)
        delay = (
            timedelta(seconds=body.delay_s)
            if body.delay_s is not None
            else None
        )
        ok = await b.requeue_dead(
            task_id,
            payload=body.payload,
            max_attempts=body.max_attempts,
            delay=delay,
        )
        return {"requeued": ok}

    @app.post("/tasks/{task_id}/purge", dependencies=[Depends(require_auth)])
    async def task_purge(task_id: int, request: Request) -> dict[str, bool]:
        b = _get_broker(request)
        ok = await b.purge_dead(task_id)
        return {"purged": ok}

    return app
