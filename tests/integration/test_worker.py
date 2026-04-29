from __future__ import annotations

import asyncio
import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Optional
from uuid import UUID

import pytest
from pydantic import BaseModel

from taskq.backoff import MAX_DELAY_S
from taskq.errors import FatalError, RetriableError
from taskq.models import Task
from taskq.registry import HandlerRegistry
from taskq.worker import worker_loop

pytestmark = pytest.mark.integration


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _make_task(
    *,
    task_id: int = 1,
    task_type: str = "demo",
    payload: Optional[dict] = None,
    timeout_s: int = 60,
    attempts: int = 1,
    max_attempts: int = 5,
) -> Task:
    return Task(
        id=task_id,
        queue="default",
        task_type=task_type,
        payload=payload if payload is not None else {},
        status="RUNNING",
        attempts=attempts,
        max_attempts=max_attempts,
        available_at=_now(),
        lease_id=uuid.uuid4(),
        timeout_s=timeout_s,
        last_error=None,
        idempotency_key=None,
        locked_by="test",
        created_at=_now(),
        updated_at=_now(),
        completed_at=None,
        visible_until=_now() + timedelta(seconds=timeout_s),
    )


class FakeBroker:
    """In-memory broker stand-in that emulates the Broker API surface.

    Mirrors §6 / §10.4 / §19.7: dispatch increments attempts; retry/kill keep attempts.
    Used to test the worker without requiring Queue Core's SQL implementation.
    """

    def __init__(self) -> None:
        self.queue: list[Task] = []
        self.tasks: dict[int, Task] = {}
        self.events: list[tuple[str, dict]] = []
        self.heartbeat_returns: list[bool] = []
        self.heartbeat_calls: int = 0

    def add(self, task: Task) -> None:
        self.tasks[task.id] = task
        self.queue.append(task)

    async def claim(self, queue: str, worker_id: str) -> Optional[Task]:
        if not self.queue:
            return None
        task = self.queue.pop(0)
        task = replace(task, status="RUNNING", locked_by=worker_id)
        self.tasks[task.id] = task
        return task

    async def ack(self, task_id: int, lease_id: UUID) -> bool:
        t = self.tasks[task_id]
        if t.lease_id != lease_id:
            return False
        self.tasks[task_id] = replace(t, status="SUCCEEDED", completed_at=_now())
        self.events.append(("ack", {"task_id": task_id}))
        return True

    async def retry(
        self,
        task_id: int,
        lease_id: UUID,
        *,
        delay: timedelta,
        error: str,
    ) -> bool:
        t = self.tasks[task_id]
        if t.lease_id != lease_id:
            return False
        if t.attempts >= t.max_attempts:
            self.tasks[task_id] = replace(
                t, status="DEAD", last_error=error, completed_at=_now()
            )
            self.events.append(("dead", {"task_id": task_id, "error": error}))
            return True
        new_available = _now() + delay
        self.tasks[task_id] = replace(
            t,
            status="PENDING",
            available_at=new_available,
            visible_until=None,
            last_error=error,
        )
        self.events.append(
            ("retry", {"task_id": task_id, "delay_s": delay.total_seconds(), "error": error})
        )
        return True

    async def kill(self, task_id: int, lease_id: UUID, *, error: str) -> bool:
        t = self.tasks[task_id]
        if t.lease_id != lease_id:
            return False
        self.tasks[task_id] = replace(
            t, status="DEAD", last_error=error, completed_at=_now()
        )
        self.events.append(("kill", {"task_id": task_id, "error": error}))
        return True

    async def heartbeat(self, task_id: int, lease_id: UUID, extend_s: int) -> bool:
        self.heartbeat_calls += 1
        if self.heartbeat_returns:
            return self.heartbeat_returns.pop(0)
        return True


class DemoPayload(BaseModel):
    n: int = 0


@pytest.mark.asyncio
async def test_worker_happy_path() -> None:
    broker = FakeBroker()
    reg = HandlerRegistry()
    ran = []

    @reg.handler("demo", payload_model=DemoPayload, timeout_s=2)
    async def demo(p: DemoPayload) -> None:
        ran.append(p.n)

    broker.add(_make_task(task_type="demo", payload={"n": 7}, timeout_s=2))

    shutdown = asyncio.Event()

    async def shut_down_after_processing():
        # Wait until the queue is consumed.
        while broker.queue:
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.05)
        shutdown.set()

    await asyncio.gather(
        worker_loop(broker, reg, "default", "w1", shutdown, poll_interval_s=0.01),
        shut_down_after_processing(),
    )

    assert ran == [7]
    assert ("ack", {"task_id": 1}) in broker.events
    assert broker.tasks[1].status == "SUCCEEDED"


@pytest.mark.asyncio
async def test_worker_handler_timeout_retried() -> None:
    broker = FakeBroker()
    reg = HandlerRegistry()

    @reg.handler("hangs", payload_model=DemoPayload, timeout_s=1)
    async def hangs(p: DemoPayload) -> None:
        await asyncio.sleep(60)

    task = _make_task(task_type="hangs", timeout_s=1, attempts=1, max_attempts=5)
    broker.add(task)

    shutdown = asyncio.Event()

    async def stop_when_retry_seen():
        while not any(e[0] == "retry" for e in broker.events):
            await asyncio.sleep(0.05)
        shutdown.set()

    await asyncio.gather(
        worker_loop(broker, reg, "default", "w1", shutdown, poll_interval_s=0.01),
        stop_when_retry_seen(),
    )

    retries = [e for e in broker.events if e[0] == "retry"]
    assert len(retries) == 1
    assert retries[0][1]["error"] == "task timeout"
    assert broker.tasks[task.id].status == "PENDING"


@pytest.mark.asyncio
async def test_worker_lease_loss_cancels_handler() -> None:
    broker = FakeBroker()
    reg = HandlerRegistry()
    cancelled = asyncio.Event()

    @reg.handler("long", payload_model=DemoPayload, timeout_s=3)
    async def long(p: DemoPayload) -> None:
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    # First heartbeat returns False -> handler should be cancelled.
    broker.heartbeat_returns = [False]
    task = _make_task(task_type="long", timeout_s=3)
    broker.add(task)

    shutdown = asyncio.Event()

    async def stop_when_cancelled():
        await asyncio.wait_for(cancelled.wait(), timeout=5)
        # Give the worker a moment to finish the iteration.
        await asyncio.sleep(0.1)
        shutdown.set()

    await asyncio.gather(
        worker_loop(broker, reg, "default", "w1", shutdown, poll_interval_s=0.01),
        stop_when_cancelled(),
    )

    assert cancelled.is_set()
    assert broker.heartbeat_calls >= 1
    # No ack/retry/kill should have happened: the lease is gone, another worker owns it.
    assert all(e[0] not in ("ack", "kill") for e in broker.events)


@pytest.mark.asyncio
async def test_worker_retriable_error_schedules_future_available_at() -> None:
    broker = FakeBroker()
    reg = HandlerRegistry()

    @reg.handler("flaky", payload_model=DemoPayload, timeout_s=2)
    async def flaky(p: DemoPayload) -> None:
        raise RetriableError("upstream 500")

    task = _make_task(task_type="flaky", attempts=1, max_attempts=5)
    broker.add(task)

    shutdown = asyncio.Event()

    async def stop_when_retry_seen():
        while not any(e[0] == "retry" for e in broker.events):
            await asyncio.sleep(0.02)
        shutdown.set()

    await asyncio.gather(
        worker_loop(broker, reg, "default", "w1", shutdown, poll_interval_s=0.01),
        stop_when_retry_seen(),
    )

    retries = [e for e in broker.events if e[0] == "retry"]
    assert len(retries) == 1
    assert retries[0][1]["delay_s"] >= 2.0  # attempts=1 lower bound
    assert broker.tasks[task.id].status == "PENDING"
    assert broker.tasks[task.id].available_at > _now() - timedelta(seconds=1)


@pytest.mark.asyncio
async def test_worker_fatal_error_kills_task() -> None:
    broker = FakeBroker()
    reg = HandlerRegistry()

    @reg.handler("bad", payload_model=DemoPayload, timeout_s=2)
    async def bad(p: DemoPayload) -> None:
        raise FatalError("nope")

    task = _make_task(task_type="bad")
    broker.add(task)

    shutdown = asyncio.Event()

    async def stop_when_dead():
        while not any(e[0] == "kill" for e in broker.events):
            await asyncio.sleep(0.02)
        shutdown.set()

    await asyncio.gather(
        worker_loop(broker, reg, "default", "w1", shutdown, poll_interval_s=0.01),
        stop_when_dead(),
    )

    assert broker.tasks[task.id].status == "DEAD"
    assert broker.tasks[task.id].last_error == "nope"


@pytest.mark.asyncio
async def test_worker_validation_error_is_fatal() -> None:
    broker = FakeBroker()
    reg = HandlerRegistry()

    @reg.handler("strict", payload_model=DemoPayload, timeout_s=2)
    async def strict(p: DemoPayload) -> None:
        return None

    # `n` must be int — pass a non-coercible value to force ValidationError.
    bad_task = _make_task(task_type="strict", payload={"n": "not-an-int!"})
    broker.add(bad_task)

    shutdown = asyncio.Event()

    async def stop_when_dead():
        while not any(e[0] == "kill" for e in broker.events):
            await asyncio.sleep(0.02)
        shutdown.set()

    await asyncio.gather(
        worker_loop(broker, reg, "default", "w1", shutdown, poll_interval_s=0.01),
        stop_when_dead(),
    )

    assert broker.tasks[bad_task.id].status == "DEAD"
    assert "validation" in (broker.tasks[bad_task.id].last_error or "").lower()


@pytest.mark.asyncio
async def test_worker_retry_after_caps_at_max_delay_s() -> None:
    broker = FakeBroker()
    reg = HandlerRegistry()

    @reg.handler("rl", payload_model=DemoPayload, timeout_s=2)
    async def rl(p: DemoPayload) -> None:
        err = RetriableError("rate limited")
        err.retry_after_seconds = 86400  # 1 day
        raise err

    task = _make_task(task_type="rl", attempts=1, max_attempts=5)
    broker.add(task)

    shutdown = asyncio.Event()

    async def stop_when_retry():
        while not any(e[0] == "retry" for e in broker.events):
            await asyncio.sleep(0.02)
        shutdown.set()

    await asyncio.gather(
        worker_loop(broker, reg, "default", "w1", shutdown, poll_interval_s=0.01),
        stop_when_retry(),
    )

    retries = [e for e in broker.events if e[0] == "retry"]
    assert len(retries) == 1
    # Retry-After=86400 must be clamped at MAX_DELAY_S (600s).
    assert retries[0][1]["delay_s"] == MAX_DELAY_S


@pytest.mark.asyncio
async def test_worker_unknown_task_type_kills() -> None:
    broker = FakeBroker()
    reg = HandlerRegistry()  # no handlers registered

    task = _make_task(task_type="nonexistent")
    broker.add(task)

    shutdown = asyncio.Event()

    async def stop_when_dead():
        while not any(e[0] == "kill" for e in broker.events):
            await asyncio.sleep(0.02)
        shutdown.set()

    await asyncio.gather(
        worker_loop(broker, reg, "default", "w1", shutdown, poll_interval_s=0.01),
        stop_when_dead(),
    )

    assert broker.tasks[task.id].status == "DEAD"
