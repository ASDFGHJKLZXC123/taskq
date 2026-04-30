# Task Queue Service

A Postgres-backed task queue with distributed workers, retries, and a dead-letter queue — built from scratch in Python. Inspired by Celery, AWS SQS, and RQ.

> **Project goal:** Build a production-shaped task queue that demonstrates real backend depth: concurrency, distributed coordination, durability guarantees, and operational maturity. Most of what makes this project valuable is the *thinking* behind each design choice — not the lines of code.

---

## 1. Why this project

Most backend systems eventually need to do work *outside* of a request/response cycle: send a confirmation email, generate a report, reindex a search document, charge a card on a schedule. A task queue is the standard answer, and almost every serious backend service uses one.

Building one yourself forces you to learn things that are usually hidden behind a library:

- **Delivery guarantees.** What does "at-least-once" actually mean, and why is exactly-once a lie?
- **Concurrency.** How do multiple workers grab tasks without stepping on each other?
- **Failure modes.** What happens when a worker crashes mid-task? When the broker restarts? When a task fails forever? When a task hangs?
- **Backoff and retries.** Why exponential backoff, why jitter, when to give up.
- **Idempotency.** Why "at-least-once" only works if your tasks tolerate being run twice — and how you actually enforce that.

It's the most "backend-y" project on a portfolio: no UI, no business logic to hide behind — just systems thinking. Building it in Python specifically forces you to confront the GIL (see §10).

### Concrete demo: webhook delivery

A queue is infrastructure. To make it legible to a recruiter or interviewer, build a **webhook delivery service** on top of it.

The queue API accepts webhook delivery requests, stores them, and enqueues delivery tasks. Workers POST the payload to subscriber URLs with retry-with-backoff. This mirrors systems like Stripe, GitHub, and Shopify without needing any external API keys.

For local development, the subscriber is another small FastAPI service in the same `docker-compose.yml`. It receives POSTs, supports deterministic failures from request payload fields, can randomly return failures for demos, records the `Idempotency-Key` header, and logs successful deliveries. The full demo should run with one command and no outside credentials:

```
docker compose up
```

This gives a concrete demo path: enqueue a webhook, watch the worker retry failed HTTP deliveries, inspect dead tasks through the admin API, optionally edit a bad payload on requeue, and verify the subscriber receives the final successful delivery once.

Failure controls:

- `force_fail_count` makes the subscriber fail the first N delivery attempts for deterministic retry tests.
- `force_status` lets tests or demos request a specific HTTP status such as `500`, `503`, or `429`.
- `Retry-After` should be honored for `429` responses when present, overriding the normal exponential backoff delay.
- `FAIL_RATE` is an environment variable for random demo failures. Tests should prefer deterministic payload flags so they do not flake.

---

## 2. Core concepts

Get these terms locked down before you start. You will use them in interviews.

**Task.** A unit of work to be performed asynchronously. Carries a `type` (which handler runs it) and a `payload` (the input data). Example: `type="send_email"`, `payload={"to": "...", "subject": "..."}`.

**Queue.** A named channel that holds pending tasks. Producers push, workers pop. Multiple queues let you separate priorities or workloads (e.g. `emails`, `reports`, `critical`).

**Producer.** Anything that submits a task. Usually your web app's request handler.

**Broker.** The component that stores queued tasks and hands them out to workers. In our system, this is backed by Postgres.

**Worker.** A long-running process that pulls tasks from a queue and executes them. Typically you run several worker processes per machine, and several machines.

**Lease.** When a worker claims a task, the broker issues a lease — a unique ID tied to that specific claim. The worker must present the lease to ack, retry, or extend. If the lease has been superseded (because another worker reclaimed the task after a timeout), the operation is rejected. This is sometimes called a "fencing token."

**Visibility timeout.** When a worker claims a task, the task becomes invisible to other workers until `visible_until`. If the worker doesn't ack by then, the broker assumes the worker crashed and the task becomes claimable again with a new lease.

**Acknowledgment (ack).** A worker signals "I finished this task successfully." The row's status flips to `SUCCEEDED` (we keep history rather than delete — see §4).

**Retry.** When a handler raises a retriable error, the task is rescheduled with a delay. `attempts` increments. Status returns to `PENDING`.

**Dead-letter queue (DLQ).** After `max_attempts` failures, a task's status flips to `DEAD` and it stops auto-retrying. Humans review dead tasks to decide whether to fix the bug, fix the data, or drop them.

**Idempotency.** Running the same task twice produces the same result as running it once. Required because at-least-once delivery means duplicates *will* happen. See §11 for how to enforce it.

---

## 3. Architecture

```
┌────────────┐    enqueue    ┌──────────────┐   poll/claim   ┌────────────┐
│  Producer  │ ────────────▶ │              │ ◀───────────── │  Worker 1  │
└────────────┘               │              │   ack/nack     └────────────┘
                             │    Broker    │ ─────────────▶ ┌────────────┐
┌────────────┐    enqueue    │  (Postgres)  │                │  Worker 2  │
│  Producer  │ ────────────▶ │              │ ◀───────────── └────────────┘
└────────────┘               │              │                ┌────────────┐
                             └──────┬───────┘ ◀───────────── │  Worker N  │
                                    │                        └────────────┘
                                    │  status flips to DEAD after max_attempts
                                    ▼
                              (rows where status='DEAD' = the DLQ)
```

A single Postgres database is the source of truth. Producers write rows, workers claim and update them, the DLQ is just a status value on a row. The broker is single-node; the *workers* are distributed across machines and coordinate via Postgres.

---

## 4. Data model

A single table is enough.

```sql
CREATE TABLE tasks (
    id              BIGSERIAL PRIMARY KEY,
    queue           TEXT      NOT NULL,
    task_type       TEXT      NOT NULL,
    payload         JSONB     NOT NULL,
    status          TEXT      NOT NULL,  -- PENDING, RUNNING, SUCCEEDED, DEAD
    attempts        INT       NOT NULL DEFAULT 0,
    max_attempts    INT       NOT NULL DEFAULT 5,
    available_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    visible_until   TIMESTAMPTZ,
    locked_by       TEXT,                -- worker id, NULL if not claimed
    lease_id        UUID,                -- regenerated on every claim; NULL if not claimed
    idempotency_key TEXT,                -- producer-supplied; nullable
    last_error      TEXT,
    timeout_s       INT       NOT NULL DEFAULT 60,   -- per-task execution cap
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ
);

-- The dispatch index. Partial because we never query SUCCEEDED/DEAD here.
CREATE INDEX idx_tasks_dispatch
    ON tasks (queue, available_at)
    WHERE status IN ('PENDING', 'RUNNING');

-- Producer-side idempotency: same key in same queue = same task.
CREATE UNIQUE INDEX idx_tasks_idempotency
    ON tasks (queue, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- For the cleanup/retention job (see §13).
CREATE INDEX idx_tasks_completed_at
    ON tasks (completed_at)
    WHERE status IN ('SUCCEEDED', 'DEAD');
```

Notes worth defending in an interview:

- **Status set is exactly four values.** `PENDING → RUNNING → (SUCCEEDED | DEAD)`, with `RUNNING → PENDING` on retry. No `FAILED`. A "failed but will retry" state is just `PENDING` with a future `available_at`.
- **`lease_id` is the fencing token.** A late worker presenting a stale lease will be rejected by `ack`/`heartbeat` (see §6).
- **`available_at`** lets us implement *delayed* and *retried* tasks for free. A task isn't eligible until `available_at <= NOW()`.
- **`visible_until`** is the visibility-timeout deadline. If it has passed and the task is still `RUNNING`, another worker can steal it. We don't need a dedicated index on `visible_until`: the partial index above covers `RUNNING` rows, and the count of `RUNNING` rows is bounded by `(workers × concurrency)` — small enough to filter in memory. If profiling later shows expired-`RUNNING` scans are slow (e.g. a bug has left many stale `RUNNING` rows), add a dedicated partial index on `(queue, visible_until) WHERE status = 'RUNNING'`.
- **`idempotency_key`** is the producer-side idempotency mechanism. Handler-side idempotency is separate (see §11).
- **`timeout_s`** is per-task. Different task types need different limits.
- **Payload size limit.** Document a hard cap (e.g., 64 KB) and reject larger payloads in `enqueue`. For larger data, store a reference (e.g., S3 URL) in the payload and let the handler fetch it.

---

## 5. The dispatch query — the heart of the system

This single SQL statement atomically claims one ready task with no contention between workers.

```sql
UPDATE tasks
SET status        = 'RUNNING',
    locked_by     = $1,
    lease_id      = gen_random_uuid(),
    visible_until = NOW() + make_interval(secs => timeout_s),
    attempts      = attempts + 1,
    updated_at    = NOW()
WHERE id = (
    SELECT id
    FROM tasks
    WHERE queue = $2
      AND available_at <= NOW()
      AND (status = 'PENDING'
           OR (status = 'RUNNING' AND visible_until < NOW()))
    ORDER BY available_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING *;
```

The magic is `FOR UPDATE SKIP LOCKED`. If 50 workers run this query simultaneously, each one grabs a *different* row without blocking. Workers that find no available row return immediately and sleep before the next poll.

The inner `WHERE` clause covers two cases in one expression:

1. A fresh task that's ready to run (`status = 'PENDING'`).
2. A previously-claimed task whose worker died (`status = 'RUNNING' AND visible_until < NOW()`). Automatic crash recovery.

`gen_random_uuid()` generates a fresh `lease_id` on every claim. Any worker still holding the *previous* lease will fail the ack — its work is no longer authoritative.

This is the same technique used by Procrastinate (Python), Solid Queue (Rails), and Que (Ruby).

---

## 6. Worker lifecycle

A worker is a small loop. With `asyncio`, a single worker process can run many tasks concurrently:

```python
async def worker_loop(worker_id: str, queue: str):
    while not shutdown_event.is_set():
        task = await broker.claim(queue, worker_id)
        if task is None:
            await asyncio.sleep(poll_interval + random.random())
            continue

        handler = registry.get(task.task_type)
        heartbeat = asyncio.create_task(
            heartbeat_loop(task.id, task.lease_id, task.timeout_s)
        )
        try:
            await asyncio.wait_for(handler(task.payload), timeout=task.timeout_s)
            await broker.ack(task.id, task.lease_id)
        except asyncio.TimeoutError:
            await broker.retry(task.id, task.lease_id,
                               delay=compute_backoff(task.attempts),
                               error="task timeout")
        except RetriableError as e:
            await broker.retry(task.id, task.lease_id,
                               delay=compute_backoff(task.attempts), error=str(e))
        except FatalError as e:
            await broker.kill(task.id, task.lease_id, error=str(e))
        except Exception as e:
            await broker.retry(task.id, task.lease_id,
                               delay=compute_backoff(task.attempts), error=str(e))
        finally:
            heartbeat.cancel()
```

In production you run a *pool* of these coroutines per process, and a pool of processes per machine — see §10.

Four things to get right:

**Lease checks on every state-changing call.** `ack`, `retry`, `kill`, and `heartbeat` all take `(task_id, lease_id)` and update only if the row's `lease_id` still matches:

```sql
UPDATE tasks
SET status = 'SUCCEEDED', completed_at = NOW(), updated_at = NOW(),
    locked_by = NULL, lease_id = NULL
WHERE id = $1 AND lease_id = $2
RETURNING id;
```

If the `RETURNING` is empty, the lease was stolen — log it and move on. Worker A finishing late cannot ack Worker B's work.

**Heartbeats for long tasks.** A background coroutine extends `visible_until` every `(timeout_s / 3)` seconds. If the heartbeat itself fails the lease check, the worker should cancel the task — its claim is gone.

**Per-task execution timeout.** `asyncio.wait_for(handler(payload), timeout=task.timeout_s)` ensures a hung handler can't run forever. Without it, heartbeats would happily extend a hung task indefinitely.

**Graceful shutdown.** On `SIGTERM`, set `shutdown_event`, stop claiming new tasks, await in-flight ones, then exit. Kubernetes default `terminationGracePeriodSeconds` of 30s is usually enough; if your tasks run longer, raise it.

---

## 7. Retry strategy

Naïve approach: retry immediately. This is wrong. If the failure was caused by a downstream service being briefly down, you'll just hammer it.

**Exponential backoff with jitter:**

```python
import random

BASE_DELAY_S = 1.0
MAX_DELAY_S  = 600.0  # 10 minutes
MAX_SHIFT    = 10

def compute_backoff(attempts: int) -> float:
    backoff = BASE_DELAY_S * (2 ** min(attempts, MAX_SHIFT))
    jitter  = random.uniform(0, backoff / 2)
    return min(backoff + jitter, MAX_DELAY_S)
```

For `BASE_DELAY_S = 1.0`:

| Attempt | Backoff window |
|--------:|---------------:|
| 1       | 1–1.5s         |
| 2       | 2–3s           |
| 3       | 4–6s           |
| 4       | 8–12s          |
| 5       | 16–24s         |

Why jitter? Without it, if 1,000 tasks fail at the same moment (downstream API blipped), all 1,000 retries fire at the *exact same time* — the so-called thundering herd. AWS published a famous post on this called "Exponential Backoff and Jitter" — worth reading and worth name-dropping.

**Cap the maximum delay.** Without it, a task that has failed 20 times might not retry for years.

**Distinguish retriable from fatal.** A 503 from a downstream is retriable. A `pydantic.ValidationError` is not — the payload is malformed, retrying won't fix it. Skip straight to the DLQ.

---

## 8. Why Postgres instead of Redis or RabbitMQ?

Every interviewer will ask this. Have a real answer.

**Pros of Postgres-as-broker:**

- One fewer system to run, monitor, and back up.
- Transactional enqueue: in your application, you can `INSERT INTO tasks ...` in the *same transaction* as the business write. Either both happen or neither. This eliminates a whole category of bugs ("we charged the card but never sent the receipt"). Celery with a Redis broker cannot do this.
- Easy to inspect with SQL. You can answer "how many email tasks are stuck?" in 10 seconds.
- Durability is free — Postgres already handles it.

**Cons:**

- Throughput ceiling is lower than Redis (~thousands/sec vs hundreds of thousands/sec).
- Polling causes constant baseline load. Mitigation: `LISTEN`/`NOTIFY` to wake workers on enqueue.
- Long-held row locks can interact badly with vacuum if you're not careful.

For most applications below "Twitter scale," Postgres is the right answer. For your portfolio project, it's the right answer because it leads to more interesting design conversations.

This is exactly the design choice **Procrastinate** (Python) made — read its source, it's small and well-written.

---

## 9. API surface

```python
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional
from uuid import UUID

@dataclass
class Task:
    id: int
    queue: str
    task_type: str
    payload: dict[str, Any]
    status: str
    attempts: int
    max_attempts: int
    available_at: datetime
    lease_id: Optional[UUID]
    timeout_s: int
    last_error: Optional[str]


class TaskQueue:
    # Producer
    async def enqueue(
        self, queue: str, task_type: str, payload: dict,
        idempotency_key: Optional[str] = None,
        delay: Optional[timedelta] = None,
        timeout_s: int = 60,
        max_attempts: int = 5,
    ) -> int: ...

    # Worker (used by the worker runtime, not user code)
    async def claim(self, queue: str, worker_id: str) -> Optional[Task]: ...
    async def ack(self, task_id: int, lease_id: UUID) -> bool: ...
    async def retry(self, task_id: int, lease_id: UUID,
                    delay: timedelta, error: str) -> bool: ...
    async def kill(self, task_id: int, lease_id: UUID, error: str) -> bool: ...
    async def heartbeat(self, task_id: int, lease_id: UUID,
                        extend_s: int) -> bool: ...

    # Operator
    async def list_dead(
        self, queue: str, limit: int = 100, before_id: Optional[int] = None
    ) -> list[Task]: ...
    async def requeue_dead(
        self, task_id: int, payload: Optional[dict] = None
    ) -> None: ...
    async def purge_dead(self, task_id: int) -> None: ...

    # Maintenance
    async def reap_completed(self, older_than: timedelta) -> int: ...
```

All worker-facing calls return `bool` — `True` if the lease was still valid, `False` if it had been superseded. Loggers should record the `False` case; it usually means visibility timeout was set too low.

User-defined handlers register through a decorator:

```python
from pydantic import BaseModel

class EmailPayload(BaseModel):
    to: str
    subject: str
    body: str

@registry.handler("send_email", payload_model=EmailPayload, timeout_s=30)
async def send_email(payload: EmailPayload) -> None:
    await mailer.send(payload.to, payload.subject, payload.body)
```

The decorator stores the function in a registry keyed by task type. At dispatch time the worker validates the JSON payload against the declared Pydantic model; validation failure raises `FatalError` and goes straight to the DLQ.

---

## 10. Tech stack

- **Python 3.12+**
- **asyncio** for worker concurrency
- **asyncpg** — fastest Postgres driver in Python; supports `LISTEN`/`NOTIFY` natively. Use raw `asyncpg` for the dispatch query and worker hot path. Admin endpoints may use raw `asyncpg` too; SQLAlchemy Core is acceptable only if it reduces boilerplate. Do not use SQLAlchemy ORM.
- **FastAPI** for the admin endpoints
- **Pydantic v2** for payload validation
- **Alembic** for migrations, written mostly as raw SQL strings so the partial indexes and Postgres-specific details stay explicit and reviewable.
- **prometheus-client** for metrics
- **structlog** for structured logging
- **pytest + pytest-asyncio + Testcontainers** for integration tests
- **uv** for dependency management
- **Docker Compose** for local dev: Postgres, queue API/worker, and the local webhook subscriber.

Package and runtime shape:

- Use an installable `src/` package named `taskq`.
- Expose CLI entry points:
  - `taskq-worker` for the worker process.
  - `taskq-admin` for the FastAPI admin/API server.
  - `taskq` for ad-hoc commands such as enqueueing demo webhook deliveries.
- Docker Compose should run API and worker as separate services that share the same image and codebase but use different entrypoints. The worker service should be scalable independently from the API service, with two local worker replicas used to demonstrate `SKIP LOCKED`.
- Do not combine the API server and worker loop in one process.

A modest, idiomatic Python stack. "I picked the simplest thing that worked" beats a buzzword salad.

### The GIL question

Python's GIL means *one* thread executes Python bytecode at a time per process. For a task queue:

- **I/O-bound tasks** (HTTP calls, DB queries — the typical case): GIL is released during I/O. A single asyncio process handles high concurrency just fine.
- **CPU-bound tasks** (image processing, ML inference): one process saturates one core. Period.

Right answer: **multiple worker processes, each running asyncio inside.** Run `N` processes (where `N ≈ CPU cores`), and inside each, run a fixed-size pool of asyncio coroutines. Same model Celery uses with its `prefork` pool, same as Gunicorn for HTTP.

Have this answer ready in interviews.

---

## 11. Idempotency — the part that's actually hard

At-least-once delivery means duplicates *will* happen. Your handlers must produce the same result whether they run once or twice. There are two layers:

**Producer-side: the `idempotency_key` column.** If a producer calls `enqueue(idempotency_key="order-123")` twice, the second call should be a no-op. The unique index on `(queue, idempotency_key)` enforces this — a duplicate insert fails, and `enqueue` catches the violation and returns the existing task ID. Use this for things like "enqueue a confirmation email after order 123 is placed" — if your order-creation code retries, you don't want two emails.

**Handler-side: write idempotent handlers.** This is the part the queue can't do for you. The handler runs at-least-once, so it must tolerate running twice. Three common patterns:

1. **Natural idempotency.** "Set the user's last-login-at to T" is idempotent by construction. Prefer this when you can structure the work this way.

2. **Idempotency table.** Before doing the work, insert a row keyed by the operation's natural identity (e.g., `(handler="send_email", to="alice@x.com", template="welcome", user_id=42)`). If the insert fails on the unique constraint, skip the work. Wrap the insert and the work in one transaction if both touch the same DB.

3. **External idempotency keys.** For external API calls, pass a request-scoped idempotency key (Stripe, AWS, GitHub all support this). Generate it deterministically from the task — `hash(task_id, attempt_zero=True)` works.

The doc-level rule: handlers must be written assuming they will run more than once. The queue cannot rescue a non-idempotent handler.

---

## 12. Observability

Without metrics, this project is half-finished.

- `tasks_enqueued_total{queue, task_type}` — counter
- `tasks_completed_total{queue, task_type, outcome}` — counter, outcome ∈ {success, retry, dead}
- `task_duration_seconds{queue, task_type}` — histogram
- `queue_depth{queue, status}` — gauge, sampled
- `oldest_pending_task_seconds{queue}` — gauge — **your single best alerting metric**
- `lease_lost_total{queue}` — counter — non-zero means visibility timeouts are too short

Structured logs via `structlog` on every state transition: `enqueue`, `claim`, `ack`, `retry`, `dead`, `lease_lost`. Include `task_id`, `lease_id`, `queue`, `task_type`, `attempt`. Propagate a correlation ID from the producer.

---

## 13. Retention and cleanup

Without retention, the table grows forever and the dispatch index degrades.

A periodic job (cron, or a simple async task in the worker process) should:

```sql
-- Delete SUCCEEDED tasks older than 7 days
DELETE FROM tasks
WHERE status = 'SUCCEEDED'
  AND completed_at < NOW() - INTERVAL '7 days';
```

Keep `DEAD` tasks indefinitely (or until manually purged) — they are debugging artifacts. If volume is high, archive them to a separate table or to S3 before deleting.

Run this every 5–15 minutes. Do it in batches (`LIMIT 1000`) to avoid long locks.

---

## 14. Build phases

Each phase is a working system; ship it, then add. **Phases 1–3 are the resume-complete scope** — you can stop there and have a defensible project. Phases 4–6 are stretch.

**Phase 1 — MVP.** Single queue, single worker process, no retries. Producer enqueues, worker pulls and runs. Just `enqueue`, `claim`, `ack`. Proves the core loop.

**Phase 2 — Reliability.** Lease IDs and stale-worker rejection. Visibility timeout + crash recovery. Multiple worker processes (test that `SKIP LOCKED` actually works). Per-task timeout. Graceful shutdown.

**Phase 3 — Retries + DLQ + Idempotency.** Exponential backoff with jitter. `max_attempts`. `DEAD` status. Producer-side `idempotency_key`. Minimum FastAPI admin endpoints — a DLQ without inspection tooling is half a feature:

```
GET  /health                          # k8s liveness
GET  /queues/{queue}/stats            # depth by status, oldest pending age
GET  /queues/{queue}/dead?limit=&before_id=    # paginated
POST /tasks/{task_id}/requeue         # optional JSON body to edit payload
POST /tasks/{task_id}/purge
```

The payload-edit-on-requeue is the operationally important one: the standard DLQ workflow is "task died because of a bad field; fix the field, then requeue." Without it, you have to purge and re-enqueue from scratch, which loses the audit trail.

The demo app for phase 3 is webhook delivery. The local subscriber service should be wired into `docker-compose.yml`, accept POSTs from the worker, fail randomly or by request payload, and log successful deliveries with the received idempotency key.

Do not build an admin HTML page in the phase-3 scope. The admin surface is JSON endpoints only. A small HTML dashboard can be added later on top of the same endpoints if a recorded demo needs it.

— *Stop here for resume-complete.* —

**Phase 4 — Multiple queues + delayed tasks.** Priority via separate queues. `delay` parameter on `enqueue`.

**Phase 5 — Observability and operations.** Prometheus metrics endpoint (`GET /metrics`). Structured logs with correlation IDs. The retention/cleanup job from §13. Optional: a minimal admin HTML dashboard wrapping the phase-3 endpoints (one weekend of work, useful for demos).

**Phase 6 (stretch) — Performance.** Switch polling to `LISTEN`/`NOTIFY` via asyncpg. Batch claims. Benchmark with locust. Compare numbers against Celery+Redis on the same hardware.

Difficulty escalates roughly linearly. Don't put estimated weeks on it; you'll just be wrong.

---

## 15. Testing strategy

- **Unit tests** for backoff calculator, handler registry, payload serialization.
- **Integration tests** against a real Postgres via Testcontainers. Cover:
  - enqueue → claim → ack happy path
  - visibility timeout reclaim after simulated worker death
  - **stale lease rejection** (Worker A claims, B reclaims after timeout, A's late ack returns `False` and the row stays as B left it)
  - DLQ transition after `max_attempts`
  - `SKIP LOCKED` correctness with concurrent claimers (`asyncio.gather`)
  - Producer-side idempotency: same `idempotency_key` enqueued twice yields one task
  - Handler timeout: a hung handler is killed by `asyncio.wait_for` and retried
- **Chaos tests.** Spin up 5 worker processes, enqueue 10,000 tasks, randomly `SIGKILL` a process every few seconds. Assert: **no task is lost; every task reaches a terminal state (`SUCCEEDED` or `DEAD`); duplicate side effects are prevented by the test handler's idempotency check.**

Suggested implementation order:

1. **Lease rejection** — Worker A claims, Worker B reclaims after timeout, Worker A's late ack returns `False`.
2. **Concurrent claim with `SKIP LOCKED`** — multiple workers claim a batch of tasks and each task is claimed exactly once.
3. **DLQ transition** — a handler that always raises moves the task to `DEAD` at `max_attempts`.
4. **Producer-side idempotency** — the same `idempotency_key` enqueued twice yields one task.
5. **Chaos test** — multiple workers process many tasks while worker processes are killed randomly.

Test organization:

- Unit tests must run without Docker.
- Integration tests use Testcontainers with a real Postgres and require Docker to be running.
- Keep tests split by folder or marker so `pytest -m "not integration"` runs the fast non-Docker suite.
- Use a session-scoped Postgres container fixture for integration tests, and truncate tables between tests instead of restarting Postgres per test.
- CI should run the full suite on an environment with Docker available.

The chaos test is the one that wins interviews. "I wrote a chaos test that kills random workers and verifies no task is lost — and a separate test that proves stale workers can't ack stolen tasks" is the kind of sentence interviewers remember.

---

## 16. Interview talking points

Lead with the *problem*, not the *implementation*:

> "I built a Postgres-backed task queue in Python because I wanted to deeply understand the delivery guarantees that Celery and SQS give you for free. The most interesting decision was using `SELECT ... FOR UPDATE SKIP LOCKED` for dispatch, which lets multiple workers claim tasks concurrently without contention and gives me transactional enqueue alongside business writes — something Celery with a Redis broker can't do."

Be ready to discuss:

- **At-least-once vs exactly-once.** Why exactly-once across a network is impossible. Idempotent handlers + at-least-once is the practical answer.
- **Lease IDs / fencing tokens.** Why the visibility-timeout mechanism alone isn't enough — without a lease check on `ack`, a slow worker could overwrite a stolen task's result.
- **Visibility timeouts.** Crash recovery vs. hung tasks (and why heartbeats need a separate per-task timeout to handle the latter).
- **Exponential backoff and jitter.** Thundering herd. AWS post.
- **Idempotency — both layers.** Producer-side via `idempotency_key`, handler-side via the patterns in §11.
- **DLQ philosophy.** Not a graveyard — an inbox for the on-call engineer.
- **The GIL and your concurrency model.** Processes-of-asyncio-coroutines.
- **Why not just use Celery?** Celery is great. Building your own taught me what Celery is doing under the hood and revealed design choices Celery couldn't make.
- **What you'd change for 10× scale.** Redis Streams, partition by hash, push-based dispatch.
- **What you'd change for 0.1× scale.** Run inline with `asyncio.create_task`. Don't pay the operational cost of a queue you don't need.

**Resume hygiene.** Only put on your resume what you actually built and tested. Every term in the talking points above will be probed if you list it. If you skipped phase 6, don't say "high-throughput" or quote benchmark numbers. Honesty here is good engineering.

---

## 17. Common pitfalls (and what they teach you)

- **Forgetting jitter.** First production incident: 200 retries fire in the same millisecond, kill the downstream service, cascade fails.
- **Acking before the work commits.** Ack last. If the task's side effect is in the same database, perform the business write and the task ack in one transaction. If the task calls an external service, use idempotency keys — atomicity across two systems isn't practically achievable, so you recover correctness via deduplication instead.
- **No lease checks.** Late workers happily ack stolen tasks. Silent data corruption.
- **Heartbeating without a per-task timeout.** A hung handler is heartbeated forever.
- **Long polling intervals.** A 5-second poll feels fine in dev; in prod, latency-sensitive tasks suffer. `LISTEN`/`NOTIFY` is the right answer.
- **Blocking calls inside async handlers.** One `time.sleep(5)` or `requests.get(...)` stalls every coroutine in that process. Use `asyncio.sleep`, `httpx.AsyncClient`, or `asyncio.to_thread`.
- **No cleanup job.** Works fine at 10k tasks. At 10M tasks the dispatch query takes 30 seconds.
- **Treating the DLQ as success.** A task in the DLQ is a *failure*. Alert on DLQ depth.

---

## 18. References worth reading

- Marc Brooker, "Exponential Backoff And Jitter" (AWS Architecture Blog)
- The **Procrastinate** source code — Python, Postgres-backed, small enough to read end-to-end
- Martin Kleppmann, "How to do distributed locking" — the canonical write-up on fencing tokens
- The Celery docs, especially "Tasks" and "Optimizing"
- The Que (Ruby) and Solid Queue (Rails) source
- "Designing Data-Intensive Applications," chapter 11 (Stream Processing)
- PEP 703 (free-threaded Python) for the GIL's future

---

## 19. Behavior contract

The decisions below resolve ambiguities left open by §1–§18. When this section conflicts with an earlier one, this section wins.

### 19.1 `heartbeat(task_id, lease_id, extend_s)`

Sets `visible_until = NOW() + extend_s` — refreshed from the current time, not added to the existing deadline. Filters on `status = 'RUNNING'` so a terminal task can't be heartbeated.

```sql
UPDATE tasks
SET visible_until = NOW() + make_interval(secs => $3),
    updated_at = NOW()
WHERE id = $1
  AND lease_id = $2
  AND status = 'RUNNING'
RETURNING id;
```

The worker calls heartbeat every `timeout_s / 3` seconds with `extend_s = timeout_s`. A task with `timeout_s = 60` heartbeats every 20s and resets `visible_until` to 60s from now.

An empty `RETURNING` has two distinct causes — stolen lease or task already terminal — log them with different event names so `lease_lost_total` doesn't conflate "a real bug" with "we ack'd a moment before the heartbeat fired."

### 19.2 `requeue_dead(task_id, payload=None, max_attempts=None, delay=None)`

Operates only on `DEAD` tasks. Resets the row for a fresh retry cycle. Payload edit is the operationally important field — most DLQ entries die because of a bad payload field — so it stays in the signature.

```sql
UPDATE tasks
SET status        = 'PENDING',
    attempts      = 0,
    available_at  = COALESCE($4, NOW()),
    visible_until = NULL,
    locked_by     = NULL,
    lease_id      = NULL,
    last_error    = NULL,
    completed_at  = NULL,
    payload       = COALESCE($2, payload),
    max_attempts  = COALESCE($3, max_attempts),
    updated_at    = NOW()
WHERE id = $1
  AND status = 'DEAD'
RETURNING id;
```

`max_attempts` stays unchanged unless the caller explicitly overrides it. Returns `True` if a row was updated, `False` if the task was not in `DEAD` state (e.g. someone else already requeued it).

### 19.3 Retry delay policy

```text
5xx response                : compute_backoff(attempts)
429 with Retry-After        : min(MAX_DELAY_S, max(compute_backoff(attempts), Retry-After))
429 without Retry-After     : compute_backoff(attempts)
408, 425                    : compute_backoff(attempts)
network error / timeout     : compute_backoff(attempts)
other 4xx                   : FatalError → DEAD (no retry)
```

`Retry-After` is clamped to `MAX_DELAY_S` (600s). A hostile or misconfigured server returning `Retry-After: 86400` must not pin a worker slot for a day; if the receiver really needs that long, the task can DLQ and a human can decide.

Every retry — including 429 — counts toward `max_attempts`.

### 19.4 Demo subscriber failure controls

Failure controls live in the fake subscriber service, not in the production webhook payload. State is keyed by `delivery_id` (the queue-internal identifier for one delivery attempt stream to one subscription).

```text
POST /webhook                  receiver under test
POST /admin/configure          set fail_next_n_requests, force_status, fail_rate
POST /admin/reset              clear all state — tests call this in setup
GET  /admin/deliveries         inspect what was received
```

In-memory state is acceptable for the demo. Integration tests reset state explicitly via `/admin/reset`; chaos tests do not assume state survives a subscriber restart.

`FAIL_RATE` (env var) drives random demo failures; `force_status` and `fail_next_n_requests` drive deterministic test failures. Tests prefer the deterministic path so they do not flake.

### 19.5 Webhook delivery payload contract

Producer-facing API:

```http
POST /events
Content-Type: application/json

{
  "event_type": "user.created",
  "data": { "user_id": "u_123", "email": "alice@example.com" }
}
```

The API creates one event row, looks up subscriptions for `event_type`, and enqueues one delivery task per subscription.

Internal queue task payload:

```json
{
  "delivery_id": "del_xxx",
  "subscription_id": "sub_xxx",
  "event_id": "evt_xxx",
  "target_url": "http://subscriber:9000/webhook",
  "event_type": "user.created",
  "body": { "event_id": "evt_xxx", "type": "user.created", "data": { ... } }
}
```

Worker POST request:

```http
POST {target_url}
Content-Type: application/json
X-Webhook-Id:        del_xxx
X-Webhook-Event-Id:  evt_xxx
X-Webhook-Timestamp: 2026-04-29T...Z
X-Webhook-Signature: sha256=<hmac(secret, timestamp + "." + body)>
Idempotency-Key:     del_xxx
```

`Idempotency-Key` equals `delivery_id` so receivers dedupe trivially. Signing scheme matches Stripe — easy to defend in interviews.

### 19.6 Poll interval

```text
TASKQ_POLL_INTERVAL_S = 0.5    base sleep when claim returns no row
sleep duration        = poll_interval + random.uniform(0, poll_interval)
```

Yields 0.5–1.0s when the queue is empty. Phase 6's `LISTEN`/`NOTIFY` upgrade replaces polling with push; until then this is the floor on enqueue-to-claim latency.

### 19.7 `attempts` semantics

`attempts` counts **claim/delivery attempts**, not handler exceptions.

```text
fresh PENDING claimed              attempts += 1   (in dispatch UPDATE)
stale RUNNING reclaimed            attempts += 1   (in dispatch UPDATE)
handler raises → retry()           no further increment
handler raises → kill() / DLQ      no further increment
```

The dispatch UPDATE in §5 already increments. `retry()` does not re-increment. The DLQ rule is: when `retry()` runs, if `attempts >= max_attempts`, the broker flips status to `DEAD` instead of `PENDING`.

This means a worker that crashes after claiming counts the same as a worker that ran the handler and got an exception — both are "we tried, we did not complete." That is the right model for at-least-once delivery.

---

## 20. Operational decisions

### 20.1 Admin API auth

Bearer token via `TASKQ_ADMIN_TOKEN` env var on every admin endpoint except `/health`.

```text
TASKQ_ENV=prod, token unset      → process refuses to start
TASKQ_ENV=dev,  token unset      → auth disabled, loud warning at startup
token set                        → enforced everywhere except /health
```

Single shared secret is the simplest thing that is not embarrassing in an interview. OAuth / mTLS are yak-shaving for a portfolio project.

### 20.2 Worker process model in Docker

One OS process per container replica. Each replica runs one asyncio event loop with `TASKQ_CONCURRENCY` coroutines. Horizontal scaling is `docker compose up --scale worker=N`.

```text
worker replica = 1 OS process = 1 asyncio loop = TASKQ_CONCURRENCY coroutines
```

This matches the container convention (one process per container). The §10 "N processes per machine" answer is the *bare-metal* shape; in containers, the container is the process boundary. Document the trade-off in the README so the interview answer reads:

> "In Docker I run one asyncio process per container and scale by replica count. On bare metal I'd run a process pool inside each machine. Same model — processes-of-coroutines — at different boundaries."

### 20.3 DB connection pool

```text
asyncpg pool per process
  TASKQ_DB_POOL_MIN = 2
  TASKQ_DB_POOL_MAX = 10
```

Sizing rule for the README: `pool_max ≥ TASKQ_CONCURRENCY + 2`. The `+2` covers the heartbeat coroutine and headroom for admin reads. Default Postgres `max_connections = 100` fits ~8 worker replicas at concurrency 10 with room left for the API service.

### 20.4 `taskq` CLI subcommands

```text
taskq enqueue --queue Q --type T --payload JSON [--idempotency-key K] [--delay-s N] [--timeout-s N] [--max-attempts N]
taskq webhook send --url URL --event-type T --data JSON
taskq dlq list --queue Q [--limit N] [--before-id ID]
taskq dlq requeue TASK_ID [--payload JSON] [--max-attempts N] [--delay-s N]
taskq dlq purge TASK_ID
taskq stats --queue Q
```

`taskq webhook send` is the recorded-demo command. Everything else is a thin wrapper over §9's API surface so demos and operations don't require curl.

### 20.5 `reap_completed` scheduling

Runs as a background asyncio task in the **API service**, not in the worker. The worker's hot path stays lean; the API process already has a long-running event loop and runs once per deployment.

```text
TASKQ_REAP_ENABLED        = true
TASKQ_REAP_INTERVAL_S     = 300       5 minutes between sweeps
TASKQ_REAP_BATCH          = 1000      LIMIT per DELETE to avoid long locks
TASKQ_REAP_RETENTION_DAYS = 7         §13 default
```

`TASKQ_REAP_ENABLED=false` for tests so cleanup does not race assertions about `SUCCEEDED` rows. `DEAD` tasks are never reaped automatically — they are debugging artifacts (§13).

---

## 21. Conventions

Defaults so the spec stays unambiguous. Override anywhere it makes sense.

**Module layout:**

```text
src/taskq/
  settings.py        env-driven config (pydantic-settings)
  db.py              asyncpg pool, migration runner
  models.py          dataclasses (Task, etc.)
  broker.py          enqueue / claim / ack / retry / kill / heartbeat
  backoff.py         compute_backoff
  registry.py        @handler decorator + lookup
  worker.py          worker_loop + heartbeat_loop + signal handling
  admin.py           FastAPI admin app
  cli.py             taskq, taskq-worker, taskq-admin entry points
  handlers/
    webhook.py       webhook delivery handler
  subscriber/
    main.py          demo FastAPI subscriber service
migrations/          Alembic migrations
tests/
  unit/              no Docker
  integration/       Testcontainers + Postgres
  chaos/             SIGKILL chaos test
```

**Logging.** structlog. JSON renderer when `TASKQ_ENV=prod`, console renderer in dev. Log every state transition (`enqueue`, `claim`, `ack`, `retry`, `dead`, `lease_lost`) with `task_id`, `lease_id`, `queue`, `task_type`, `attempt`. Propagate a correlation ID from producer to handler logs.

**Migrations.** Alembic, but each migration's `upgrade()` / `downgrade()` is a single `op.execute(sa.text("""..."""))` block of raw SQL. Keeps partial indexes and Postgres-specific clauses verbatim and reviewable.

**httpx (webhook handler).** `connect=5s`, `read=25s`. Total budget ≤ `task.timeout_s − 5s` so backoff math has headroom. One shared `httpx.AsyncClient` per process — do not construct one per task.

**Webhook signing.** HMAC-SHA256 over `timestamp + "." + body`, secret from `TASKQ_WEBHOOK_SIGNING_SECRET`. Stripe-style. Header value is `sha256=<hex>`.

**Errors.** `RetriableError` and `FatalError` live in `taskq.errors`. Anything else thrown by a handler is treated as retriable (§6). Pydantic `ValidationError` raised during payload parsing is treated as `FatalError` — bad payload, retrying won't fix it.

---

*This is a living document. Update it as you build — the parts you change tell you what you're actually learning.*
