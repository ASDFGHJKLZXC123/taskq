from __future__ import annotations

import asyncio
import os
import random
import signal
import socket
import uuid
from datetime import timedelta
from typing import Optional

import structlog
from pydantic import ValidationError

from taskq.backoff import MAX_DELAY_S, compute_backoff
from taskq.broker import Broker
from taskq.errors import FatalError, RetriableError
from taskq.logging_config import configure_logging
from taskq.models import Task
from taskq.registry import HandlerRegistry, HandlerSpec
from taskq.registry import registry as default_registry
from taskq.settings import Settings

log = structlog.get_logger(__name__)


async def _heartbeat_loop(
    broker: Broker,
    task: Task,
    timeout_s: int,
    handler_task: asyncio.Task,
) -> None:
    interval = max(timeout_s / 3, 0.1)
    while not handler_task.done():
        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            return
        if handler_task.done():
            return
        ok = await broker.heartbeat(task.id, task.lease_id, extend_s=timeout_s)
        if not ok:
            log.warning(
                "heartbeat_lost",
                task_id=task.id,
                lease_id=str(task.lease_id),
                queue=task.queue,
                task_type=task.task_type,
                attempt=task.attempts,
            )
            handler_task.cancel()
            return


def _retry_delay(attempts: int, exc: BaseException) -> float:
    base = compute_backoff(attempts)
    retry_after = getattr(exc, "retry_after_seconds", None)
    if retry_after is not None:
        return min(MAX_DELAY_S, max(base, float(retry_after)))
    return base


async def _execute_task(
    broker: Broker,
    registry: HandlerRegistry,
    task: Task,
) -> None:
    try:
        spec: HandlerSpec = registry.get(task.task_type)
    except KeyError as exc:
        log.error(
            "no_handler",
            task_id=task.id,
            task_type=task.task_type,
            queue=task.queue,
        )
        await broker.kill(task.id, task.lease_id, error=f"no handler: {exc}")
        return

    timeout_s = spec.timeout_s

    try:
        payload_obj = spec.payload_model.model_validate(task.payload)
    except ValidationError as exc:
        log.error(
            "payload_validation_failed",
            task_id=task.id,
            task_type=task.task_type,
            queue=task.queue,
            errors=exc.errors(),
        )
        await broker.kill(task.id, task.lease_id, error=f"validation: {exc}")
        return

    handler_task = asyncio.create_task(
        asyncio.wait_for(spec.func(payload_obj), timeout=timeout_s)
    )
    heartbeat_task = asyncio.create_task(
        _heartbeat_loop(broker, task, timeout_s, handler_task)
    )

    try:
        try:
            await handler_task
        finally:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except (asyncio.CancelledError, Exception):
                pass
        await broker.ack(task.id, task.lease_id)
        log.info(
            "task_acked",
            task_id=task.id,
            task_type=task.task_type,
            queue=task.queue,
            attempt=task.attempts,
        )
    except asyncio.TimeoutError:
        log.warning(
            "task_timeout",
            task_id=task.id,
            task_type=task.task_type,
            queue=task.queue,
            attempt=task.attempts,
        )
        await broker.retry(
            task.id,
            task.lease_id,
            delay=timedelta(seconds=compute_backoff(task.attempts)),
            error="task timeout",
        )
    except asyncio.CancelledError:
        # Lease lost: heartbeat cancelled the handler. Do not ack/retry; another worker owns it.
        log.warning(
            "task_cancelled_lease_lost",
            task_id=task.id,
            task_type=task.task_type,
            queue=task.queue,
            attempt=task.attempts,
        )
    except RetriableError as exc:
        delay = _retry_delay(task.attempts, exc)
        log.info(
            "task_retry",
            task_id=task.id,
            task_type=task.task_type,
            queue=task.queue,
            attempt=task.attempts,
            delay_s=delay,
            error=str(exc),
        )
        await broker.retry(
            task.id,
            task.lease_id,
            delay=timedelta(seconds=delay),
            error=str(exc),
        )
    except FatalError as exc:
        log.warning(
            "task_fatal",
            task_id=task.id,
            task_type=task.task_type,
            queue=task.queue,
            attempt=task.attempts,
            error=str(exc),
        )
        await broker.kill(task.id, task.lease_id, error=str(exc))
    except Exception as exc:
        log.warning(
            "task_unexpected_error",
            task_id=task.id,
            task_type=task.task_type,
            queue=task.queue,
            attempt=task.attempts,
            error=repr(exc),
        )
        await broker.retry(
            task.id,
            task.lease_id,
            delay=timedelta(seconds=compute_backoff(task.attempts)),
            error=str(exc),
        )


async def worker_loop(
    broker: Broker,
    registry: HandlerRegistry,
    queue: str,
    worker_id: str,
    shutdown_event: asyncio.Event,
    poll_interval_s: float = 0.5,
) -> None:
    while not shutdown_event.is_set():
        try:
            task = await broker.claim(queue, worker_id)
        except Exception as exc:
            log.error("claim_failed", error=repr(exc), queue=queue, worker_id=worker_id)
            await asyncio.sleep(poll_interval_s)
            continue

        if task is None:
            # §19.6: jittered sleep when nothing to claim.
            await asyncio.sleep(poll_interval_s + random.random() * poll_interval_s)
            continue

        await _execute_task(broker, registry, task)


async def run_worker(settings: Optional[Settings] = None) -> None:
    settings = settings or Settings()
    configure_logging(settings.TASKQ_ENV, settings.TASKQ_LOG_LEVEL)

    from taskq.db import create_pool

    pool = await create_pool(settings)
    broker = Broker(pool)

    # Trigger handler registration as a side effect of import.
    import taskq.handlers.webhook  # noqa: F401

    worker_id = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
    shutdown_event = asyncio.Event()

    loop = asyncio.get_running_loop()

    def _request_shutdown() -> None:
        log.info("shutdown_signal_received")
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _request_shutdown)
        except NotImplementedError:
            pass

    log.info(
        "worker_starting",
        worker_id=worker_id,
        queue=settings.TASKQ_QUEUE,
        concurrency=settings.TASKQ_CONCURRENCY,
    )

    try:
        coros = [
            worker_loop(
                broker,
                default_registry,
                settings.TASKQ_QUEUE,
                worker_id,
                shutdown_event,
                poll_interval_s=settings.TASKQ_POLL_INTERVAL_S,
            )
            for _ in range(settings.TASKQ_CONCURRENCY)
        ]
        await asyncio.gather(*coros, return_exceptions=True)
    finally:
        log.info("worker_draining", worker_id=worker_id)
        await pool.close()
