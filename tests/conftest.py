"""Canonical pytest fixtures for the task queue service.

Fixtures provided:

  postgres_container  (session)  - Testcontainers Postgres 16. Skip if Docker absent.
                                   Runs `alembic upgrade head` after start.
  pool                (function) - asyncpg pool with the JSONB codec installed.
                                   Truncates `tasks` before each test.
  broker              (function) - Broker(pool).
  settings_for_test   (function) - Settings overridden with the container DSN.
  subscriber_app      (function) - taskq.subscriber.main.app, state reset.
  subscriber_client   (function) - httpx.AsyncClient wired to subscriber_app via ASGI.
  admin_client        (function) - factory that turns an ASGI app into an AsyncClient.

Tests that need a separate side-table (e.g. chaos test's `chaos_log`) can manage it
inside the test module via a local autouse fixture — see tests/integration/test_chaos.py.
"""
from __future__ import annotations

import json
import os
from typing import Any, AsyncIterator, Iterator

import asyncpg
import pytest
import pytest_asyncio


# ---------------------------------------------------------------------------
# Session-scoped Postgres container
# ---------------------------------------------------------------------------


def _docker_available() -> bool:
    try:
        import docker  # type: ignore
    except Exception:
        return False
    try:
        client = docker.from_env()
        client.ping()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def postgres_container() -> Iterator[str]:
    """Start a Postgres 16 container, run migrations, yield the DSN.

    Skips cleanly if Docker isn't available.
    """
    if not _docker_available():
        pytest.skip("Docker not available")

    from testcontainers.postgres import PostgresContainer
    from alembic import command
    from alembic.config import Config

    with PostgresContainer("postgres:16") as pg:
        url = pg.get_connection_url()
        # Testcontainers returns postgresql+psycopg2:// — strip the driver hint.
        for prefix in ("postgresql+psycopg2://", "postgresql+asyncpg://"):
            if url.startswith(prefix):
                url = url.replace(prefix, "postgresql://", 1)
                break

        # env.py reads TASKQ_DATABASE_URL from environment.
        prev = os.environ.get("TASKQ_DATABASE_URL")
        os.environ["TASKQ_DATABASE_URL"] = url
        try:
            cfg = Config("alembic.ini")
            cfg.set_main_option("sqlalchemy.url", url)
            command.upgrade(cfg, "head")
            yield url
        finally:
            if prev is None:
                os.environ.pop("TASKQ_DATABASE_URL", None)
            else:
                os.environ["TASKQ_DATABASE_URL"] = prev


# ---------------------------------------------------------------------------
# Per-test asyncpg pool
# ---------------------------------------------------------------------------


async def _init_codec(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )
    await conn.set_type_codec(
        "json",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


@pytest_asyncio.fixture
async def pool(postgres_container: str) -> AsyncIterator[asyncpg.Pool]:
    p = await asyncpg.create_pool(
        postgres_container,
        min_size=1,
        max_size=8,
        init=_init_codec,
    )
    async with p.acquire() as conn:
        await conn.execute("TRUNCATE tasks RESTART IDENTITY")
    try:
        yield p
    finally:
        await p.close()


# ---------------------------------------------------------------------------
# Broker
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def broker(pool: asyncpg.Pool):
    from taskq.broker import Broker
    return Broker(pool)


# ---------------------------------------------------------------------------
# Settings tuned for tests
# ---------------------------------------------------------------------------


@pytest.fixture
def settings_for_test(postgres_container: str):
    from taskq.settings import Settings
    return Settings(
        TASKQ_DATABASE_URL=postgres_container,
        TASKQ_PAYLOAD_MAX_BYTES=65536,
        TASKQ_REAP_ENABLED=False,
        TASKQ_ADMIN_TOKEN=None,
        TASKQ_ENV="dev",
    )


# ---------------------------------------------------------------------------
# Subscriber ASGI app
# ---------------------------------------------------------------------------


def _try_get_subscriber_app() -> Any:
    """Best-effort import of the subscriber FastAPI app.

    The subscriber lives in src/taskq/subscriber/main.py. If the Worker+Webhook
    specialist hasn't filled it in yet, we return None and tests using this fixture
    will skip cleanly.
    """
    try:
        from taskq.subscriber import main as subscriber_main
    except Exception:
        return None
    return getattr(subscriber_main, "app", None)


@pytest.fixture
def subscriber_app() -> Any:
    app = _try_get_subscriber_app()
    if app is None:
        pytest.skip("subscriber app not implemented yet (taskq.subscriber.main.app missing)")
    # Best effort to clear in-memory state between tests if a module-level container exists.
    try:
        from taskq.subscriber import main as subscriber_main
        for attr in ("_state", "state", "deliveries", "_deliveries"):
            obj = getattr(subscriber_main, attr, None)
            if obj is not None and hasattr(obj, "clear"):
                obj.clear()
    except Exception:
        pass
    return app


@pytest_asyncio.fixture
async def subscriber_client(subscriber_app):
    import httpx

    transport = httpx.ASGITransport(app=subscriber_app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://subscriber.test"
    ) as client:
        # Reset state via admin endpoint if available.
        try:
            await client.post("/admin/reset")
        except Exception:
            pass
        yield client


# ---------------------------------------------------------------------------
# Admin app client factory
# ---------------------------------------------------------------------------


@pytest.fixture
def admin_client():
    """Factory: ``async with admin_client(app) as client: ...``.

    Builds an httpx.AsyncClient over the given ASGI app.
    """
    import contextlib

    @contextlib.asynccontextmanager
    async def _factory(app):
        import httpx
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://admin.test"
        ) as client:
            yield client

    return _factory
