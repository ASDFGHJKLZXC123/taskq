from __future__ import annotations

import asyncio
import json
import sys
import uuid
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional
from uuid import UUID

import click

from taskq.broker import Broker
from taskq.db import create_pool
from taskq.logging_config import configure_logging
from taskq.settings import Settings


def _json_default(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, UUID):
        return str(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _print_json(obj: Any) -> None:
    click.echo(json.dumps(obj, default=_json_default, indent=2))


def _parse_json_input(value: str, *, what: str = "JSON") -> Any:
    if value.startswith("@"):
        path = Path(value[1:])
        try:
            text = path.read_text()
        except OSError as exc:
            raise click.BadParameter(f"could not read {what} file {path}: {exc}")
    else:
        text = value
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise click.BadParameter(f"{what} is not valid JSON: {exc}")


async def _with_broker(fn):
    settings = Settings()
    pool = await create_pool(settings)
    try:
        broker = Broker(pool, settings)
        return await fn(broker)
    finally:
        await pool.close()


def _serialize_task_dict(task) -> dict[str, Any]:
    return asdict(task)


@click.group()
def taskq() -> None:
    """taskq command-line interface."""


@taskq.command("enqueue")
@click.option("--queue", "queue", required=True)
@click.option("--type", "task_type", required=True)
@click.option("--payload", "payload", required=True, help="JSON literal or @file.json")
@click.option("--idempotency-key", "idempotency_key", default=None)
@click.option("--delay-s", "delay_s", type=int, default=None)
@click.option("--timeout-s", "timeout_s", type=int, default=60)
@click.option("--max-attempts", "max_attempts", type=int, default=5)
def cli_enqueue(
    queue: str,
    task_type: str,
    payload: str,
    idempotency_key: Optional[str],
    delay_s: Optional[int],
    timeout_s: int,
    max_attempts: int,
) -> None:
    payload_obj = _parse_json_input(payload, what="payload")
    if not isinstance(payload_obj, dict):
        raise click.BadParameter("payload must be a JSON object")

    delay = timedelta(seconds=delay_s) if delay_s else None

    async def _run(broker: Broker) -> int:
        return await broker.enqueue(
            queue,
            task_type,
            payload_obj,
            idempotency_key=idempotency_key,
            delay=delay,
            timeout_s=timeout_s,
            max_attempts=max_attempts,
        )

    task_id = asyncio.run(_with_broker(_run))
    _print_json({"id": task_id})


@taskq.group("webhook")
def cli_webhook() -> None:
    """Webhook helpers."""


@cli_webhook.command("send")
@click.option("--url", "url", required=True)
@click.option("--event-type", "event_type", required=True)
@click.option("--data", "data", required=True, help="JSON literal or @file.json")
@click.option("--queue", "queue", default="default")
def cli_webhook_send(url: str, event_type: str, data: str, queue: str) -> None:
    data_obj = _parse_json_input(data, what="data")
    if not isinstance(data_obj, dict):
        raise click.BadParameter("data must be a JSON object")

    delivery_id = f"del_{uuid.uuid4().hex}"
    event_id = f"evt_{uuid.uuid4().hex}"
    body = {"event_id": event_id, "type": event_type, "data": data_obj}
    payload = {
        "delivery_id": delivery_id,
        "subscription_id": "demo-sub",
        "event_id": event_id,
        "target_url": url,
        "event_type": event_type,
        "body": body,
    }

    async def _run(broker: Broker) -> int:
        return await broker.enqueue(queue, "webhook.deliver", payload)

    task_id = asyncio.run(_with_broker(_run))
    _print_json({"id": task_id, "delivery_id": delivery_id, "event_id": event_id})


@taskq.group("dlq")
def cli_dlq() -> None:
    """Dead-letter queue inspection."""


@cli_dlq.command("list")
@click.option("--queue", "queue", required=True)
@click.option("--limit", "limit", type=int, default=100)
@click.option("--before-id", "before_id", type=int, default=None)
def cli_dlq_list(queue: str, limit: int, before_id: Optional[int]) -> None:
    async def _run(broker: Broker):
        return await broker.list_dead(queue, limit=limit, before_id=before_id)

    tasks = asyncio.run(_with_broker(_run))
    _print_json([_serialize_task_dict(t) for t in tasks])


@cli_dlq.command("requeue")
@click.argument("task_id", type=int)
@click.option("--payload", "payload", default=None, help="JSON literal or @file.json")
@click.option("--max-attempts", "max_attempts", type=int, default=None)
@click.option("--delay-s", "delay_s", type=int, default=None)
def cli_dlq_requeue(
    task_id: int,
    payload: Optional[str],
    max_attempts: Optional[int],
    delay_s: Optional[int],
) -> None:
    payload_obj: Optional[dict] = None
    if payload is not None:
        parsed = _parse_json_input(payload, what="payload")
        if not isinstance(parsed, dict):
            raise click.BadParameter("payload must be a JSON object")
        payload_obj = parsed

    delay = timedelta(seconds=delay_s) if delay_s else None

    async def _run(broker: Broker) -> bool:
        return await broker.requeue_dead(
            task_id,
            payload=payload_obj,
            max_attempts=max_attempts,
            delay=delay,
        )

    ok = asyncio.run(_with_broker(_run))
    _print_json({"requeued": ok})


@cli_dlq.command("purge")
@click.argument("task_id", type=int)
def cli_dlq_purge(task_id: int) -> None:
    async def _run(broker: Broker) -> bool:
        return await broker.purge_dead(task_id)

    ok = asyncio.run(_with_broker(_run))
    _print_json({"purged": ok})


@taskq.command("stats")
@click.option("--queue", "queue", required=True)
def cli_stats(queue: str) -> None:
    async def _run(broker: Broker) -> dict:
        return await broker.stats(queue)

    stats = asyncio.run(_with_broker(_run))
    _print_json(stats)


@taskq.group("subscriber")
def cli_subscriber() -> None:
    """Demo subscriber service."""


@cli_subscriber.command("run")
def cli_subscriber_run() -> None:
    subscriber_main()


def taskq_main() -> None:
    taskq()


def worker_main() -> None:
    settings = Settings()
    configure_logging(settings.TASKQ_ENV, settings.TASKQ_LOG_LEVEL)
    try:
        from taskq.worker import run_worker
    except ImportError as exc:
        raise SystemExit(f"taskq.worker.run_worker not available: {exc}")
    asyncio.run(run_worker(settings))


def admin_main() -> None:
    import uvicorn

    from taskq.admin import create_app

    settings = Settings()
    configure_logging(settings.TASKQ_ENV, settings.TASKQ_LOG_LEVEL)
    app = create_app(settings=settings)
    uvicorn.run(app, host="0.0.0.0", port=8000, log_config=None)


def subscriber_main() -> None:
    settings = Settings()
    configure_logging(settings.TASKQ_ENV, settings.TASKQ_LOG_LEVEL)
    try:
        from taskq.subscriber.main import run as subscriber_run
    except ImportError as exc:
        raise SystemExit(f"taskq.subscriber.main.run not available: {exc}")
    subscriber_run()


if __name__ == "__main__":  # pragma: no cover
    sys.exit(taskq_main())
