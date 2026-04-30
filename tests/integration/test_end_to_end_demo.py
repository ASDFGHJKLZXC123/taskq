"""End-to-end recorded-demo test (§1).

Runs everything in-process to keep wall time low:

* Subscriber FastAPI app  → ASGI in-process (httpx.ASGITransport).
* Worker                  → asyncio.create_task(run_worker_loop)
* Admin app               → ASGI in-process.
* Broker / Postgres       → real, via the testcontainers-managed DSN.

Three steps mirror the recorded demo:

1. Configure subscriber to fail the first 2 requests, enqueue a delivery,
   wait, assert subscriber received exactly 1 delivery (after retries).
2. Configure subscriber to permanently fail with HTTP 400 (FatalError →
   straight to DEAD), enqueue, wait, assert task is in /dead listing.
3. Reset subscriber to success, requeue the dead task, wait, assert
   subscriber received the delivery and admin shows 0 dead tasks.

Notes on how we wire the worker's httpx.AsyncClient
---------------------------------------------------
The webhook handler caches a module-level ``httpx.AsyncClient`` per process.
We replace it with one whose ``ASGITransport`` routes ``http://subscriber.test``
to the in-process FastAPI app. ``webhook_module.reset_client()`` lets us
swap it cleanly.
"""
from __future__ import annotations

import asyncio

import httpx
import pytest
from httpx import ASGITransport

from taskq.admin import create_app as create_admin_app
from taskq.broker import Broker
from taskq.handlers import webhook as webhook_module
from taskq.registry import registry as default_registry
from taskq.settings import Settings
from taskq.subscriber import main as subscriber
from taskq.worker import worker_loop


pytestmark = pytest.mark.integration


SUBSCRIBER_URL = "http://subscriber.test/webhook"
QUEUE = "demo"


@pytest.fixture
async def wired_subscriber():
    """Patch the webhook handler's httpx client to route to the in-process subscriber.

    Yields the AsyncClient that talks to the subscriber so tests can also use it
    directly for /admin/configure and /admin/deliveries calls.
    """
    transport = ASGITransport(app=subscriber.app)
    client = httpx.AsyncClient(transport=transport, base_url="http://subscriber.test")

    await webhook_module.reset_client()
    webhook_module._client = client
    webhook_module._settings = Settings(_env_file=None)

    # Reset subscriber state.
    await client.post("/admin/reset")

    try:
        yield client
    finally:
        await webhook_module.reset_client()
        await client.aclose()


async def _wait_until(predicate, timeout_s: float = 15.0, poll_s: float = 0.2):
    """Poll ``predicate`` until it returns truthy or ``timeout_s`` elapses."""
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        result = await predicate()
        if result:
            return result
        await asyncio.sleep(poll_s)
    return None


async def _start_worker(broker: Broker, queue: str) -> tuple[asyncio.Task, asyncio.Event]:
    shutdown = asyncio.Event()
    task = asyncio.create_task(
        worker_loop(
            broker,
            default_registry,
            queue,
            "demo-worker",
            shutdown,
            poll_interval_s=0.1,
        )
    )
    return task, shutdown


async def _stop_worker(task: asyncio.Task, shutdown: asyncio.Event) -> None:
    shutdown.set()
    try:
        await asyncio.wait_for(task, timeout=5.0)
    except asyncio.TimeoutError:
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass


def _delivery_payload(delivery_id: str) -> dict:
    return {
        "delivery_id": delivery_id,
        "subscription_id": "sub_1",
        "event_id": f"evt_{delivery_id}",
        "target_url": SUBSCRIBER_URL,
        "event_type": "user.created",
        "body": {
            "event_id": f"evt_{delivery_id}",
            "type": "user.created",
            "data": {"user_id": "u_1", "email": "alice@example.com"},
        },
    }


async def test_end_to_end_demo_full_flow(broker, pool, postgres_container, wired_subscriber):
    """The full §1 webhook demo, end to end."""
    settings = Settings(
        _env_file=None,
        TASKQ_DATABASE_URL=postgres_container,
        TASKQ_REAP_ENABLED=False,
        TASKQ_ADMIN_TOKEN=None,
        TASKQ_ENV="dev",
    )

    # --- Step 1: subscriber fails the first 2 attempts; delivery succeeds on attempt 3.
    r = await wired_subscriber.post(
        "/admin/configure",
        json={"fail_next_n_requests": 2, "force_status": 503},
    )
    assert r.status_code == 200

    worker_task, shutdown = await _start_worker(broker, QUEUE)

    try:
        delivery_id_1 = "del_step1"
        # max_attempts=5 so we survive the 2 failures plus the success.
        # Use small payload to make backoff calls fast (compute_backoff is jittered;
        # window for attempt=1 is 2-3s and for attempt=2 is 4-6s — wait window must allow that).
        await broker.enqueue(
            QUEUE,
            "webhook.deliver",
            _delivery_payload(delivery_id_1),
            max_attempts=5,
            timeout_s=15,
        )

        # Wait until subscriber records exactly one successful delivery for del_step1.
        async def step1_ok():
            r = await wired_subscriber.get("/admin/deliveries")
            deliveries = r.json()
            matching = [d for d in deliveries if (d.get("body") or {}).get("event_id") == f"evt_{delivery_id_1}"]
            return matching if len(matching) >= 1 else None

        delivered = await _wait_until(step1_ok, timeout_s=30.0)
        assert delivered is not None, "no successful delivery for step 1"
        assert len(delivered) == 1, f"step 1: expected 1 delivery, got {len(delivered)}"
        # Idempotency-Key header equals delivery_id (§19.5).
        assert delivered[0]["headers"]["Idempotency-Key"] == delivery_id_1

        # --- Step 2: subscriber returns 400 forever (FatalError → DEAD on first attempt).
        await wired_subscriber.post("/admin/reset")
        await wired_subscriber.post(
            "/admin/configure",
            json={"fail_next_n_requests": 999, "force_status": 400},
        )

        delivery_id_2 = "del_step2"
        dead_task_id = await broker.enqueue(
            QUEUE,
            "webhook.deliver",
            _delivery_payload(delivery_id_2),
            max_attempts=3,
            timeout_s=10,
        )

        # Wait for dead_task_id to land in DLQ (FatalError on 4xx → straight to DEAD).
        async def step2_dead():
            dead = await broker.list_dead(QUEUE, limit=20)
            return any(t.id == dead_task_id for t in dead)

        is_dead = await _wait_until(step2_dead, timeout_s=20.0)
        assert is_dead, f"task {dead_task_id} did not reach DEAD"

        # Verify via the admin endpoint as well.
        admin_app = create_admin_app(broker=broker, settings=settings)
        async with admin_app.router.lifespan_context(admin_app):
            async with httpx.AsyncClient(
                transport=ASGITransport(app=admin_app), base_url="http://admin.test"
            ) as admin:
                r = await admin.get(f"/queues/{QUEUE}/dead")
                assert r.status_code == 200
                dead_ids = [t["id"] for t in r.json()["tasks"]]
                assert dead_task_id in dead_ids

                # --- Step 3: reset subscriber, requeue the dead task, expect successful delivery.
                await wired_subscriber.post("/admin/reset")

                r = await admin.post(
                    f"/tasks/{dead_task_id}/requeue",
                    json={"max_attempts": 5},
                )
                assert r.status_code == 200
                assert r.json() == {"requeued": True}

        async def step3_redelivered():
            r = await wired_subscriber.get("/admin/deliveries")
            for d in r.json():
                if (d.get("body") or {}).get("event_id") == f"evt_{delivery_id_2}":
                    return d
            return None

        redelivered = await _wait_until(step3_redelivered, timeout_s=20.0)
        assert redelivered is not None, "requeued task did not reach subscriber"
        assert redelivered["headers"]["Idempotency-Key"] == delivery_id_2

        # Verify DLQ is empty for this queue.
        dead = await broker.list_dead(QUEUE, limit=20)
        assert dead == [], f"expected empty DLQ, got {[t.id for t in dead]}"

    finally:
        await _stop_worker(worker_task, shutdown)
