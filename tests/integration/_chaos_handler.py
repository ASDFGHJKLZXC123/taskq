"""Idempotent test handler used by tests/integration/test_chaos.py.

Subprocess workers import this module so the ``chaos.record`` handler is
registered before they enter their main loop. The handler does an UPSERT
into the ``chaos_log`` side-table — exactly-once-effective even if a task
runs multiple times because of crash recovery.

The launcher invocation looks like::

    python -c "import taskq.handlers.webhook, tests.integration._chaos_handler;\
               from taskq.cli import worker_main; worker_main()"
"""
from __future__ import annotations

import asyncpg
from pydantic import BaseModel

from taskq.registry import registry
from taskq.settings import Settings


class ChaosPayload(BaseModel):
    i: int


@registry.handler("chaos.record", payload_model=ChaosPayload, timeout_s=10)
async def chaos_record(payload: ChaosPayload) -> None:
    """Idempotent INSERT keyed by ``payload.i``.

    The handler's idempotency check (``ON CONFLICT DO NOTHING``) is what
    guarantees count==1 even when a task is re-claimed after SIGKILL. This
    is the §11 "idempotency table" pattern — the queue runs the handler
    at-least-once, the handler tolerates running twice.
    """
    settings = Settings()
    dsn = settings.TASKQ_DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://", 1)
    conn = await asyncpg.connect(dsn)
    try:
        # Tiny sleep makes the SIGKILL window wider so the test exercises mid-
        # handler crashes; the second run is a no-op thanks to ON CONFLICT.
        await conn.execute(
            """
            INSERT INTO chaos_log (i, count) VALUES ($1, 1)
            ON CONFLICT (i) DO NOTHING
            """,
            payload.i,
        )
    finally:
        await conn.close()
