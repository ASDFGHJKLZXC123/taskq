from __future__ import annotations

import asyncio
import logging

import pytest
from httpx import ASGITransport, AsyncClient

from taskq.admin import create_app
from taskq.settings import Settings


pytestmark = pytest.mark.integration


def _make_settings(
    *,
    env: str = "dev",
    token: str | None = None,
    reap_enabled: bool = False,
    reap_interval_s: int = 300,
    reap_retention_days: int = 7,
) -> Settings:
    s = Settings(_env_file=None)
    s.TASKQ_ENV = env  # type: ignore[assignment]
    s.TASKQ_ADMIN_TOKEN = token
    s.TASKQ_REAP_ENABLED = reap_enabled
    s.TASKQ_REAP_INTERVAL_S = reap_interval_s
    s.TASKQ_REAP_RETENTION_DAYS = reap_retention_days
    return s


def _client(app):
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver")


async def test_health_no_auth(broker):
    settings = _make_settings(env="dev", token="secret")
    app = create_app(broker=broker, settings=settings)
    async with app.router.lifespan_context(app):
        async with _client(app) as client:
            r = await client.get("/health")
            assert r.status_code == 200
            assert r.json() == {"status": "ok"}


async def test_endpoints_require_bearer_when_token_set(broker):
    settings = _make_settings(env="dev", token="secret")
    app = create_app(broker=broker, settings=settings)

    async with app.router.lifespan_context(app):
        async with _client(app) as client:
            r = await client.get("/queues/default/stats")
            assert r.status_code == 401

            r = await client.get(
                "/queues/default/stats", headers={"Authorization": "Bearer wrong"}
            )
            assert r.status_code == 401

            r = await client.get(
                "/queues/default/stats", headers={"Authorization": "Bearer secret"}
            )
            assert r.status_code == 200


async def test_dev_no_token_warns_and_accepts_unauthenticated(broker):
    settings = _make_settings(env="dev", token=None)

    captured: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            captured.append(record)

    handler = _Capture(level=logging.WARNING)
    admin_logger = logging.getLogger("taskq.admin")
    # Pytest's caplog can leave loggers disabled between tests; re-enable explicitly.
    admin_logger.disabled = False
    admin_logger.setLevel(logging.WARNING)
    admin_logger.addHandler(handler)
    try:
        app = create_app(broker=broker, settings=settings)
    finally:
        admin_logger.removeHandler(handler)

    assert any(
        rec.levelno == logging.WARNING and "TASKQ_ADMIN_TOKEN" in rec.getMessage()
        for rec in captured
    ), [(r.levelno, r.getMessage()) for r in captured]

    async with app.router.lifespan_context(app):
        async with _client(app) as client:
            r = await client.get("/queues/default/stats")
            assert r.status_code == 200


async def test_prod_no_token_raises():
    settings = _make_settings(env="prod", token=None)
    with pytest.raises(RuntimeError, match="admin token required"):
        create_app(broker=None, settings=settings)


async def test_stats_shape(broker):
    settings = _make_settings(env="dev", token=None)
    app = create_app(broker=broker, settings=settings)

    await broker.enqueue("default", "noop", {})
    await broker.enqueue("default", "noop", {})

    async with app.router.lifespan_context(app):
        async with _client(app) as client:
            r = await client.get("/queues/default/stats")
            assert r.status_code == 200
            body = r.json()
            assert "depth_by_status" in body
            assert body["depth_by_status"]["PENDING"] == 2
            assert "oldest_pending_age_seconds" in body


async def test_dead_paginates_via_before_id(broker):
    settings = _make_settings(env="dev", token=None)
    app = create_app(broker=broker, settings=settings)

    ids = []
    for i in range(5):
        tid = await broker.enqueue("default", "noop", {"i": i})
        ids.append(tid)
        claimed = await broker.claim("default", "worker-1")
        assert claimed is not None
        await broker.kill(claimed.id, claimed.lease_id, error="x")

    async with app.router.lifespan_context(app):
        async with _client(app) as client:
            r = await client.get("/queues/default/dead", params={"limit": 2})
            assert r.status_code == 200
            tasks = r.json()["tasks"]
            assert [t["id"] for t in tasks] == [ids[4], ids[3]]
            assert isinstance(tasks[0]["lease_id"], (str, type(None)))
            assert isinstance(tasks[0]["available_at"], str)

            last_id = tasks[-1]["id"]
            r2 = await client.get(
                "/queues/default/dead", params={"limit": 2, "before_id": last_id}
            )
            assert r2.status_code == 200
            tasks2 = r2.json()["tasks"]
            assert [t["id"] for t in tasks2] == [ids[2], ids[1]]


async def test_requeue_dead_returns_true_and_applies_payload(broker, pool):
    settings = _make_settings(env="dev", token=None)
    app = create_app(broker=broker, settings=settings)

    tid = await broker.enqueue("default", "noop", {"orig": True})
    claimed = await broker.claim("default", "worker-1")
    assert claimed is not None
    await broker.kill(claimed.id, claimed.lease_id, error="x")

    async with app.router.lifespan_context(app):
        async with _client(app) as client:
            r = await client.post(
                f"/tasks/{tid}/requeue",
                json={"payload": {"edited": True}, "max_attempts": 7},
            )
            assert r.status_code == 200
            assert r.json() == {"requeued": True}

    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM tasks WHERE id = $1", tid)
    assert row["status"] == "PENDING"
    assert row["max_attempts"] == 7
    payload = row["payload"]
    if isinstance(payload, str):
        import json
        payload = json.loads(payload)
    assert payload == {"edited": True}


async def test_requeue_non_dead_returns_false(broker):
    settings = _make_settings(env="dev", token=None)
    app = create_app(broker=broker, settings=settings)

    tid = await broker.enqueue("default", "noop", {})

    async with app.router.lifespan_context(app):
        async with _client(app) as client:
            r = await client.post(f"/tasks/{tid}/requeue", json={})
            assert r.status_code == 200
            assert r.json() == {"requeued": False}


async def test_purge_dead_deletes_row(broker, pool):
    settings = _make_settings(env="dev", token=None)
    app = create_app(broker=broker, settings=settings)

    tid = await broker.enqueue("default", "noop", {})
    claimed = await broker.claim("default", "worker-1")
    assert claimed is not None
    await broker.kill(claimed.id, claimed.lease_id, error="x")

    async with app.router.lifespan_context(app):
        async with _client(app) as client:
            r = await client.post(f"/tasks/{tid}/purge")
            assert r.status_code == 200
            assert r.json() == {"purged": True}

    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM tasks WHERE id = $1", tid)
    assert row is None


async def test_reap_loop_cleans_old_succeeded(broker, pool):
    settings = _make_settings(
        env="dev",
        token=None,
        reap_enabled=True,
        reap_interval_s=1,
        reap_retention_days=0,
    )
    app = create_app(broker=broker, settings=settings)

    tid = await broker.enqueue("default", "noop", {})
    claimed = await broker.claim("default", "worker-1")
    assert claimed is not None
    await broker.ack(claimed.id, claimed.lease_id)

    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE tasks SET completed_at = NOW() - INTERVAL '1 day' WHERE id = $1",
            tid,
        )

    async with app.router.lifespan_context(app):
        # Lifespan startup spawns reap_loop; wait long enough for at least one sweep.
        await asyncio.sleep(2.0)

        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM tasks WHERE id = $1", tid)
        assert row is None

    # After lifespan shutdown the reap task is cancelled and gathered cleanly.
    reap_task = app.state.reap_task
    assert reap_task is not None
    assert reap_task.done()
