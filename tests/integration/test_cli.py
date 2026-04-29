from __future__ import annotations

import asyncio
import json
from typing import Any, Awaitable, Callable, TypeVar

import asyncpg
import pytest
from click.testing import CliRunner

from taskq.broker import Broker
from taskq.cli import taskq
from taskq.settings import Settings


pytestmark = pytest.mark.integration

T = TypeVar("T")


def _run_cli(args: list[str]):
    runner = CliRunner()
    return runner.invoke(taskq, args, catch_exceptions=False)


def _async(coro_factory: Callable[[], Awaitable[T]]) -> T:
    """Run a coroutine in a private event loop (CLI tests are sync)."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro_factory())
    finally:
        loop.close()


@pytest.fixture
def fresh_db(postgres_container: str) -> str:
    """Truncate tasks before each CLI test; the CLI opens its own pool."""

    async def _truncate() -> None:
        conn = await asyncpg.connect(postgres_container)
        try:
            await conn.execute("TRUNCATE TABLE tasks RESTART IDENTITY")
        finally:
            await conn.close()

    _async(_truncate)
    return postgres_container


def _broker_call(coro_factory: Callable[[Broker], Awaitable[T]]) -> T:
    async def _run() -> T:
        settings = Settings(_env_file=None)
        from taskq.db import create_pool

        pool = await create_pool(settings)
        try:
            broker = Broker(pool, settings)
            return await coro_factory(broker)
        finally:
            await pool.close()

    return _async(_run)


def _fetchrow(dsn: str, sql: str, *args: Any):
    async def _run():
        conn = await asyncpg.connect(dsn)
        try:
            return await conn.fetchrow(sql, *args)
        finally:
            await conn.close()

    return _async(_run)


def test_enqueue_returns_id(fresh_db):
    result = _run_cli(["enqueue", "--queue", "q", "--type", "t", "--payload", "{}"])
    assert result.exit_code == 0, result.output
    body = json.loads(result.output)
    assert isinstance(body["id"], int)
    assert body["id"] > 0


def test_stats_prints_json(fresh_db):
    _broker_call(lambda b: b.enqueue("q", "t", {}))

    result = _run_cli(["stats", "--queue", "q"])
    assert result.exit_code == 0, result.output
    body = json.loads(result.output)
    assert "depth_by_status" in body
    assert body["depth_by_status"]["PENDING"] >= 1


def test_dlq_list_initially_empty(fresh_db):
    result = _run_cli(["dlq", "list", "--queue", "q"])
    assert result.exit_code == 0, result.output
    body = json.loads(result.output)
    assert body == []


def test_dlq_requeue_works_on_dead_row(fresh_db):
    async def _setup(b: Broker) -> int:
        tid = await b.enqueue("q", "t", {"orig": True})
        claimed = await b.claim("q", "worker-1")
        assert claimed is not None
        await b.kill(claimed.id, claimed.lease_id, error="x")
        return tid

    tid = _broker_call(_setup)

    result = _run_cli(["dlq", "requeue", str(tid)])
    assert result.exit_code == 0, result.output
    body = json.loads(result.output)
    assert body == {"requeued": True}

    row = _fetchrow(fresh_db, "SELECT status FROM tasks WHERE id = $1", tid)
    assert row["status"] == "PENDING"


def test_dlq_purge_works(fresh_db):
    async def _setup(b: Broker) -> int:
        tid = await b.enqueue("q", "t", {})
        claimed = await b.claim("q", "worker-1")
        assert claimed is not None
        await b.kill(claimed.id, claimed.lease_id, error="x")
        return tid

    tid = _broker_call(_setup)

    result = _run_cli(["dlq", "purge", str(tid)])
    assert result.exit_code == 0, result.output
    body = json.loads(result.output)
    assert body == {"purged": True}

    row = _fetchrow(fresh_db, "SELECT id FROM tasks WHERE id = $1", tid)
    assert row is None


def test_webhook_send_enqueues_webhook_deliver(fresh_db):
    result = _run_cli(
        [
            "webhook",
            "send",
            "--url",
            "http://example.com/x",
            "--event-type",
            "test.event",
            "--data",
            "{}",
            "--queue",
            "q",
        ]
    )
    assert result.exit_code == 0, result.output
    body = json.loads(result.output)
    assert isinstance(body["id"], int)
    assert body["delivery_id"].startswith("del_")
    assert body["event_id"].startswith("evt_")

    row = _fetchrow(
        fresh_db,
        "SELECT task_type, payload FROM tasks WHERE id = $1",
        body["id"],
    )
    assert row["task_type"] == "webhook.deliver"
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    assert payload["target_url"] == "http://example.com/x"
    assert payload["event_type"] == "test.event"
    assert payload["delivery_id"] == body["delivery_id"]
    assert payload["body"]["data"] == {}
