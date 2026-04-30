from __future__ import annotations

import asyncio
import json
from datetime import timedelta

import pytest


pytestmark = pytest.mark.integration


async def _row(pool, task_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM tasks WHERE id = $1", task_id)


async def test_enqueue_claim_ack_happy_path(broker, pool):
    task_id = await broker.enqueue("default", "noop", {"x": 1})
    assert task_id > 0

    claimed = await broker.claim("default", "worker-1")
    assert claimed is not None
    assert claimed.id == task_id
    assert claimed.status == "RUNNING"
    assert claimed.lease_id is not None
    assert claimed.attempts == 1

    ok = await broker.ack(claimed.id, claimed.lease_id)
    assert ok is True

    row = await _row(pool, task_id)
    assert row["status"] == "SUCCEEDED"
    assert row["completed_at"] is not None
    assert row["lease_id"] is None
    assert row["locked_by"] is None


async def test_enqueue_idempotency_returns_same_id(broker):
    a = await broker.enqueue("default", "noop", {"a": 1}, idempotency_key="dup-1")
    b = await broker.enqueue("default", "noop", {"a": 2}, idempotency_key="dup-1")
    assert a == b


async def test_enqueue_rejects_oversize_payload(broker):
    from taskq.settings import get_settings

    cap = get_settings().TASKQ_PAYLOAD_MAX_BYTES
    big_blob = "x" * (cap + 100)
    with pytest.raises(ValueError, match="TASKQ_PAYLOAD_MAX_BYTES"):
        await broker.enqueue("default", "noop", {"blob": big_blob})


async def test_enqueue_with_delay_not_claimable_until_ready(broker, pool):
    task_id = await broker.enqueue(
        "default", "noop", {}, delay=timedelta(seconds=60)
    )
    initial = await broker.claim("default", "worker-1")
    assert initial is None

    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE tasks SET available_at = NOW() - INTERVAL '1 second' WHERE id = $1",
            task_id,
        )

    claimed = await broker.claim("default", "worker-1")
    assert claimed is not None
    assert claimed.id == task_id


async def test_claim_returns_none_on_empty_queue(broker):
    result = await broker.claim("default", "worker-1")
    assert result is None


async def test_stale_lease_ack_returns_false_row_keeps_b_lease(broker, pool):
    task_id = await broker.enqueue("default", "noop", {})
    a_claim = await broker.claim("default", "worker-A")
    assert a_claim is not None
    a_lease = a_claim.lease_id

    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE tasks SET visible_until = NOW() - INTERVAL '1 second' WHERE id = $1",
            task_id,
        )

    b_claim = await broker.claim("default", "worker-B")
    assert b_claim is not None
    assert b_claim.id == task_id
    b_lease = b_claim.lease_id
    assert b_lease != a_lease

    a_ack = await broker.ack(task_id, a_lease)
    assert a_ack is False

    row = await _row(pool, task_id)
    assert row["lease_id"] == b_lease
    assert row["status"] == "RUNNING"


async def test_skip_locked_concurrent_claims_distinct(broker):
    n = 20
    for i in range(n):
        await broker.enqueue("default", "noop", {"i": i})

    results = await asyncio.gather(
        *[broker.claim("default", f"w-{i}") for i in range(n)]
    )

    assert all(r is not None for r in results)
    ids = {r.id for r in results}
    assert len(ids) == n


async def test_retry_when_attempts_below_max_returns_to_pending(broker, pool):
    task_id = await broker.enqueue("default", "noop", {}, max_attempts=5)
    claimed = await broker.claim("default", "worker-1")
    assert claimed is not None
    assert claimed.attempts == 1

    ok = await broker.retry(
        claimed.id, claimed.lease_id,
        delay=timedelta(seconds=30),
        error="boom",
    )
    assert ok is True

    row = await _row(pool, task_id)
    assert row["status"] == "PENDING"
    assert row["last_error"] == "boom"
    assert row["lease_id"] is None
    assert row["locked_by"] is None
    assert row["available_at"] > row["created_at"]


async def test_retry_at_max_attempts_flips_to_dead(broker, pool):
    task_id = await broker.enqueue("default", "noop", {}, max_attempts=1)
    claimed = await broker.claim("default", "worker-1")
    assert claimed is not None
    assert claimed.attempts == 1
    assert claimed.attempts >= claimed.max_attempts

    ok = await broker.retry(
        claimed.id, claimed.lease_id,
        delay=timedelta(seconds=10),
        error="dead-now",
    )
    assert ok is True

    row = await _row(pool, task_id)
    assert row["status"] == "DEAD"
    assert row["last_error"] == "dead-now"
    assert row["lease_id"] is None
    assert row["locked_by"] is None
    assert row["completed_at"] is not None


async def test_kill_flips_to_dead_with_last_error(broker, pool):
    task_id = await broker.enqueue("default", "noop", {})
    claimed = await broker.claim("default", "worker-1")
    assert claimed is not None

    ok = await broker.kill(claimed.id, claimed.lease_id, error="fatal!")
    assert ok is True

    row = await _row(pool, task_id)
    assert row["status"] == "DEAD"
    assert row["last_error"] == "fatal!"
    assert row["lease_id"] is None
    assert row["locked_by"] is None
    assert row["completed_at"] is not None


async def test_heartbeat_extends_visible_until_iff_lease_matches(broker, pool):
    task_id = await broker.enqueue("default", "noop", {}, timeout_s=30)
    claimed = await broker.claim("default", "worker-1")
    assert claimed is not None

    before = await _row(pool, task_id)
    initial_visible = before["visible_until"]

    await asyncio.sleep(0.05)

    ok = await broker.heartbeat(claimed.id, claimed.lease_id, extend_s=120)
    assert ok is True

    after = await _row(pool, task_id)
    assert after["visible_until"] > initial_visible

    from uuid import uuid4

    bogus = uuid4()
    ok2 = await broker.heartbeat(claimed.id, bogus, extend_s=120)
    assert ok2 is False


async def test_heartbeat_returns_false_on_terminal_status(broker):
    await broker.enqueue("default", "noop", {})
    claimed = await broker.claim("default", "worker-1")
    assert claimed is not None

    await broker.ack(claimed.id, claimed.lease_id)

    ok = await broker.heartbeat(claimed.id, claimed.lease_id, extend_s=60)
    assert ok is False

    await broker.enqueue("default", "noop", {})
    claimed_2 = await broker.claim("default", "worker-1")
    assert claimed_2 is not None
    await broker.kill(claimed_2.id, claimed_2.lease_id, error="x")

    ok2 = await broker.heartbeat(claimed_2.id, claimed_2.lease_id, extend_s=60)
    assert ok2 is False


async def test_requeue_dead_resets_with_overrides(broker, pool):
    task_id = await broker.enqueue("default", "noop", {"orig": True}, max_attempts=1)
    claimed = await broker.claim("default", "worker-1")
    assert claimed is not None
    await broker.retry(claimed.id, claimed.lease_id, delay=timedelta(seconds=0), error="boom")

    row = await _row(pool, task_id)
    assert row["status"] == "DEAD"

    new_payload = {"orig": False, "edited": True}
    ok = await broker.requeue_dead(
        task_id,
        payload=new_payload,
        max_attempts=10,
        delay=timedelta(seconds=120),
    )
    assert ok is True

    row = await _row(pool, task_id)
    assert row["status"] == "PENDING"
    assert row["attempts"] == 0
    assert row["max_attempts"] == 10
    assert row["last_error"] is None
    assert row["completed_at"] is None
    assert row["lease_id"] is None
    assert row["locked_by"] is None
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    assert payload == new_payload
    assert row["available_at"] > row["updated_at"] - timedelta(seconds=1)


async def test_requeue_dead_on_non_dead_returns_false(broker):
    task_id = await broker.enqueue("default", "noop", {})
    ok = await broker.requeue_dead(task_id)
    assert ok is False


async def test_purge_dead_only_purges_dead_rows(broker, pool):
    task_id = await broker.enqueue("default", "noop", {})
    ok = await broker.purge_dead(task_id)
    assert ok is False
    row = await _row(pool, task_id)
    assert row is not None

    claimed = await broker.claim("default", "worker-1")
    assert claimed is not None
    await broker.kill(claimed.id, claimed.lease_id, error="x")

    ok2 = await broker.purge_dead(task_id)
    assert ok2 is True
    row2 = await _row(pool, task_id)
    assert row2 is None


async def test_reap_completed_deletes_old_succeeded_only(broker, pool):
    old_succ_id = await broker.enqueue("default", "noop", {"x": "old_succ"})
    claimed = await broker.claim("default", "worker-1")
    assert claimed is not None
    await broker.ack(claimed.id, claimed.lease_id)

    fresh_succ_id = await broker.enqueue("default", "noop", {"x": "fresh"})
    claimed_fresh = await broker.claim("default", "worker-1")
    assert claimed_fresh is not None
    await broker.ack(claimed_fresh.id, claimed_fresh.lease_id)

    dead_id = await broker.enqueue("default", "noop", {"x": "dead"})
    claimed_dead = await broker.claim("default", "worker-1")
    assert claimed_dead is not None
    await broker.kill(claimed_dead.id, claimed_dead.lease_id, error="x")

    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE tasks SET completed_at = NOW() - INTERVAL '10 days' WHERE id = $1",
            old_succ_id,
        )
        await conn.execute(
            "UPDATE tasks SET completed_at = NOW() - INTERVAL '10 days' WHERE id = $1",
            dead_id,
        )

    deleted = await broker.reap_completed(timedelta(days=7))
    assert deleted == 1

    assert await _row(pool, old_succ_id) is None
    assert await _row(pool, fresh_succ_id) is not None
    assert await _row(pool, dead_id) is not None


async def test_list_dead_paginates_via_before_id(broker):
    ids = []
    for i in range(5):
        tid = await broker.enqueue("default", "noop", {"i": i})
        ids.append(tid)
        claimed = await broker.claim("default", "worker-1")
        assert claimed is not None
        await broker.kill(claimed.id, claimed.lease_id, error="x")

    page1 = await broker.list_dead("default", limit=2)
    assert [t.id for t in page1] == [ids[4], ids[3]]

    page2 = await broker.list_dead("default", limit=2, before_id=page1[-1].id)
    assert [t.id for t in page2] == [ids[2], ids[1]]

    page3 = await broker.list_dead("default", limit=2, before_id=page2[-1].id)
    assert [t.id for t in page3] == [ids[0]]
