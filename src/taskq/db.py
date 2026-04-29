from __future__ import annotations

import json

import asyncpg

from taskq.settings import Settings


def _strip_driver(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


async def _init_connection(conn: asyncpg.Connection) -> None:
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


async def create_pool(settings: Settings) -> asyncpg.Pool:
    dsn = _strip_driver(settings.TASKQ_DATABASE_URL)
    return await asyncpg.create_pool(
        dsn=dsn,
        min_size=settings.TASKQ_DB_POOL_MIN,
        max_size=settings.TASKQ_DB_POOL_MAX,
        init=_init_connection,
    )
