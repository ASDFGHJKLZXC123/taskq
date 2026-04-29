"""Chaos test (§15).

Spawn N worker subprocesses, enqueue 1000 ``chaos.record`` tasks, ``SIGKILL``
random workers every second for ~30 seconds, then drain. Assert:

- Every task is in a terminal state (SUCCEEDED or DEAD).
- No tasks remain PENDING or RUNNING.
- The ``chaos_log`` side-table has count==1 for every SUCCEEDED task
  (effectively-once semantics: the queue is at-least-once, the handler is
  written idempotently with ``ON CONFLICT DO NOTHING``).

Subprocess setup
----------------
Each worker subprocess imports ``tests.integration._chaos_handler`` so the
``chaos.record`` handler is registered, then drops into ``worker_main``.
The launcher uses ``python -c`` because we need to patch the registry before
the worker enters its claim loop. Documented in ``_chaos_handler.py``.
"""
from __future__ import annotations

import asyncio
import os
import random
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import List

import pytest


pytestmark = [pytest.mark.integration, pytest.mark.chaos]


N_WORKERS = 5
N_TASKS = 1000
KILL_DURATION_S = 30
DRAIN_TIMEOUT_S = 60
QUEUE = "chaos"


# Path to repo root — needed so `python -c "..."` can find both packages.
_REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
async def chaos_log_table(pool):
    """Create the `chaos_log` side-table for the test, drop after."""
    async with pool.acquire() as conn:
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS chaos_log (i INT PRIMARY KEY, count INT NOT NULL)"
        )
        await conn.execute("TRUNCATE chaos_log")
    yield
    async with pool.acquire() as conn:
        await conn.execute("DROP TABLE IF EXISTS chaos_log")


def _spawn_worker(dsn: str) -> subprocess.Popen:
    """Spawn a worker process with the chaos handler registered.

    We invoke the registered ``worker_main`` after explicitly importing the
    chaos handler module so the registry has ``chaos.record`` before the
    claim loop starts.
    """
    bootstrap = (
        "import sys; "
        f"sys.path.insert(0, {str(_REPO_ROOT)!r}); "
        "import taskq.handlers.webhook; "
        "import tests.integration._chaos_handler; "
        "from taskq.cli import worker_main; "
        "worker_main()"
    )
    env = {
        **os.environ,
        "TASKQ_DATABASE_URL": dsn,
        "TASKQ_QUEUE": QUEUE,
        "TASKQ_CONCURRENCY": "4",
        "TASKQ_POLL_INTERVAL_S": "0.2",
        "TASKQ_REAP_ENABLED": "false",
        "TASKQ_LOG_LEVEL": "WARNING",
        "PYTHONUNBUFFERED": "1",
    }
    return subprocess.Popen(
        [sys.executable, "-c", bootstrap],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _kill_all(procs: List[subprocess.Popen]) -> None:
    for p in procs:
        if p.poll() is None:
            try:
                p.kill()
            except ProcessLookupError:
                pass
    for p in procs:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass


async def test_chaos_no_loss_no_duplicate_effects(
    broker, pool, postgres_container, chaos_log_table
):
    """Worker SIGKILLs must not lose tasks; idempotent handler must produce count=1 each."""

    # Phase 1: enqueue 1000 chaos tasks.
    enqueue_tasks = [
        broker.enqueue(QUEUE, "chaos.record", {"i": i}, max_attempts=10, timeout_s=10)
        for i in range(N_TASKS)
    ]
    enqueued = await asyncio.gather(*enqueue_tasks)
    assert len(set(enqueued)) == N_TASKS

    # Phase 2: spawn N workers.
    procs: List[subprocess.Popen] = [_spawn_worker(postgres_container) for _ in range(N_WORKERS)]

    # Phase 3: kill random workers every ~1s for KILL_DURATION_S, replacing as we go.
    kill_deadline = time.monotonic() + KILL_DURATION_S
    try:
        while time.monotonic() < kill_deadline:
            await asyncio.sleep(1.0)
            # Pick a live victim and kill it.
            alive = [p for p in procs if p.poll() is None]
            if not alive:
                break
            victim = random.choice(alive)
            try:
                os.kill(victim.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            # Replace it.
            try:
                victim.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pass
            procs = [p for p in procs if p is not victim]
            procs.append(_spawn_worker(postgres_container))

        # Phase 4: stop killing, let workers drain.
        drain_deadline = time.monotonic() + DRAIN_TIMEOUT_S
        while time.monotonic() < drain_deadline:
            stats = await broker.stats(QUEUE)
            depth = stats["depth_by_status"]
            if depth["PENDING"] == 0 and depth["RUNNING"] == 0:
                break
            await asyncio.sleep(1.0)
        else:
            pytest.fail(f"queue did not drain in {DRAIN_TIMEOUT_S}s; stats={stats}")

    finally:
        _kill_all(procs)

    # Phase 5: assertions.
    stats = await broker.stats(QUEUE)
    depth = stats["depth_by_status"]
    assert depth["PENDING"] == 0, depth
    assert depth["RUNNING"] == 0, depth
    assert depth["SUCCEEDED"] + depth["DEAD"] == N_TASKS, depth

    # Idempotent handler: every task index must have produced exactly one row,
    # even though the same task may have been claimed multiple times after SIGKILL.
    async with pool.acquire() as conn:
        chaos_rows = await conn.fetch("SELECT i, count FROM chaos_log ORDER BY i")
        # For tasks that ended up DEAD, there's no chaos_log row — this is fine.
        succeeded_ids = {
            r["i"]
            for r in await conn.fetch(
                "SELECT (payload->>'i')::int AS i FROM tasks "
                "WHERE queue = $1 AND status = 'SUCCEEDED'",
                QUEUE,
            )
        }

    chaos_by_i = {r["i"]: r["count"] for r in chaos_rows}

    # Every SUCCEEDED task must appear in chaos_log with count == 1.
    missing = succeeded_ids - chaos_by_i.keys()
    assert not missing, f"SUCCEEDED tasks missing from chaos_log: {sorted(missing)[:20]}"

    over_one = {i: c for i, c in chaos_by_i.items() if c != 1}
    assert not over_one, f"chaos_log has duplicates (handler not idempotent?): {over_one}"
