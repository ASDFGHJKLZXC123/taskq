# Team Plan — Task Queue Service

The Lead Architect (you, today) has scaffolded the repo. Each specialist owns the files
listed below and should edit ONLY those files. Read teammate WIP if helpful, do not modify it.

---

## 1. Scope

**In scope: §14 Phases 1–3.**

- Phase 1 — MVP: enqueue / claim / ack, single queue, single worker.
- Phase 2 — Reliability: lease IDs, visibility timeout / crash recovery, multiple worker
  processes (`SKIP LOCKED` correctness), per-task timeout, graceful shutdown.
- Phase 3 — Retries + DLQ + Idempotency: exponential backoff with jitter, `max_attempts`,
  `DEAD` status, producer-side `idempotency_key`, FastAPI admin endpoints (JSON only),
  webhook delivery demo + local subscriber service.

**Out of scope (do NOT build):**

- LISTEN / NOTIFY (Phase 6).
- Prometheus `/metrics` endpoint or `prometheus-client` (Phase 5). `structlog` is fine —
  it is a logging choice, lightweight, already wired in `logging_config.py`.
- Admin HTML page (Phase 5 stretch).
- Multiple priority queues beyond what the `queue` parameter on `enqueue` already provides
  (Phase 4 — separate priority queues).
- Delayed tasks via a separate scheduler (the `delay` parameter on `enqueue` is in scope,
  but no Phase 4 priority-queue plumbing).
- Retention/cleanup as a separately scheduled cron — `reap_completed` is implemented, but
  any heavier retention (Phase 5) stays out.
- Any benchmarking or LISTEN/NOTIFY perf work (Phase 6).

---

## 2. File ownership map

| Path                                                     | Owner             |
|----------------------------------------------------------|-------------------|
| `pyproject.toml`                                         | Lead Architect    |
| `.gitignore`, `.env.example`                             | Lead Architect    |
| `Dockerfile`                                             | Lead Architect    |
| `docker-compose.yml`                                     | Lead Architect (final tweaks: Testing/DevEx) |
| `alembic.ini`, `migrations/env.py`, `migrations/script.py.mako`, `migrations/README` | Lead Architect |
| `migrations/versions/`                                   | Queue Core        |
| `src/taskq/__init__.py`                                  | Lead Architect    |
| `src/taskq/settings.py`                                  | Lead Architect    |
| `src/taskq/models.py`                                    | Lead Architect    |
| `src/taskq/errors.py`                                    | Lead Architect    |
| `src/taskq/logging_config.py`                            | Lead Architect    |
| `src/taskq/db.py`                                        | Queue Core        |
| `src/taskq/broker.py`                                    | Queue Core        |
| `src/taskq/worker.py`                                    | Worker + Webhook  |
| `src/taskq/backoff.py`                                   | Worker + Webhook  |
| `src/taskq/registry.py`                                  | Worker + Webhook  |
| `src/taskq/handlers/__init__.py`                         | Worker + Webhook  |
| `src/taskq/handlers/webhook.py`                          | Worker + Webhook  |
| `src/taskq/subscriber/__init__.py`                       | Worker + Webhook  |
| `src/taskq/subscriber/main.py`                           | Worker + Webhook  |
| `src/taskq/admin.py`                                     | API + CLI         |
| `src/taskq/cli.py`                                       | API + CLI         |
| `tests/conftest.py`                                      | Testing + DevEx   |
| `tests/unit/`                                            | mixed (see §11)   |
| `tests/integration/`                                     | mixed (see §11)   |
| `tests/chaos/`                                           | Testing + DevEx   |
| `README.md`                                              | Testing + DevEx   |
| CI config (`.github/workflows/...`)                      | Testing + DevEx   |
| `TEAM_PLAN.md` (this file)                               | Lead Architect (broadcasts changes) |

The `reap_completed` background task that runs inside the API service is API+CLI's job to
wire up; the SQL itself is Queue Core's `Broker.reap_completed`.

---

## 3. Build / run commands

```bash
# Local install (editable, with dev extras)
python3.12 -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"

# Migrations (requires a reachable Postgres at TASKQ_DATABASE_URL)
alembic upgrade head

# Tests
pytest                       # all
pytest -m "not integration"  # fast suite, no Docker
pytest -m integration        # Testcontainers + Postgres
pytest -m chaos              # long-running

# Docker stack
docker compose up --build
docker compose up --scale worker=2  # required to demo SKIP LOCKED
docker compose down -v
```

Entry points (after `pip install -e .`):

```bash
taskq-admin       # FastAPI admin/API server (uvicorn under the hood, port 8000)
taskq-worker      # worker process
taskq             # ad-hoc CLI: enqueue / dlq / stats / webhook send
```

---

## 4. Data model (§4)

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

Status set is exactly four values: `PENDING → RUNNING → (SUCCEEDED | DEAD)`, with
`RUNNING → PENDING` on retry. There is no `FAILED` state.

Per §4: enforce `TASKQ_PAYLOAD_MAX_BYTES` (default 64 KB) in `Broker.enqueue` — reject
larger payloads.

---

## 5. Dispatch query (§5)

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

`gen_random_uuid()` generates a fresh `lease_id` per claim. The dispatch UPDATE is the
ONE place `attempts` is incremented (§19.7).

---

## 6. Lease-checked SQL

### 6.1 ack (§6)

```sql
UPDATE tasks
SET status = 'SUCCEEDED', completed_at = NOW(), updated_at = NOW(),
    locked_by = NULL, lease_id = NULL
WHERE id = $1 AND lease_id = $2
RETURNING id;
```

### 6.2 heartbeat (§19.1)

```sql
UPDATE tasks
SET visible_until = NOW() + make_interval(secs => $3),
    updated_at = NOW()
WHERE id = $1
  AND lease_id = $2
  AND status = 'RUNNING'
RETURNING id;
```

Heartbeat refreshes from the current time, NOT from the existing deadline. Filter on
`status = 'RUNNING'` so terminal tasks can't be heartbeated. Empty `RETURNING` has two
distinct causes — stolen lease vs. already-terminal — log them as different events so
`lease_lost_total` doesn't conflate them.

### 6.3 requeue_dead (§19.2)

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

### 6.4 retry / kill (Queue Core writes the SQL; this is the contract)

`retry(task_id, lease_id, *, delay, error)`:
- Lease check on `lease_id`.
- Status filter `'RUNNING'`.
- Read `attempts` and `max_attempts` from the row (after the dispatch UPDATE has
  already incremented `attempts`).
- If `attempts >= max_attempts` → flip to `DEAD`, set `last_error`, `completed_at = NOW()`,
  clear `locked_by` and `lease_id`.
- Else → flip to `PENDING`, set `available_at = NOW() + delay`, `visible_until = NULL`,
  `last_error = $error`, clear `locked_by` and `lease_id`. Do NOT increment `attempts`.
- Return `True` if a row was updated, `False` if the lease was stolen or the row is no
  longer `RUNNING`.

`kill(task_id, lease_id, *, error)`:
- Lease check, status filter `'RUNNING'`.
- Flip to `DEAD`, set `last_error = $error`, `completed_at = NOW()`, clear lease fields.

### 6.5 purge_dead

```sql
DELETE FROM tasks WHERE id = $1 AND status = 'DEAD' RETURNING id;
```

### 6.6 reap_completed (§13, §20.5)

```sql
DELETE FROM tasks
WHERE id IN (
    SELECT id FROM tasks
    WHERE status = 'SUCCEEDED'
      AND completed_at < NOW() - make_interval(secs => $1)
    LIMIT $2
);
```

`$1` = `older_than` seconds, `$2` = `TASKQ_REAP_BATCH`. Never deletes `DEAD` rows.

---

## 7. `Task` dataclass

Defined in `src/taskq/models.py`. Fields:

```text
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
idempotency_key: Optional[str]
locked_by: Optional[str]
created_at: datetime
updated_at: datetime
completed_at: Optional[datetime]
visible_until: Optional[datetime]
```

`Task.from_record(record)` maps an `asyncpg.Record` (or any mapping) to a `Task`. Use it
in every Broker method that returns a Task.

---

## 8. `Broker` interface

In `src/taskq/broker.py`. Method signatures (Queue Core implements bodies):

```python
async def enqueue(self, queue: str, task_type: str, payload: dict, *,
                  idempotency_key: Optional[str] = None,
                  delay: Optional[timedelta] = None,
                  timeout_s: int = 60,
                  max_attempts: int = 5) -> int: ...
async def claim(self, queue: str, worker_id: str) -> Optional[Task]: ...
async def ack(self, task_id: int, lease_id: UUID) -> bool: ...
async def retry(self, task_id: int, lease_id: UUID, *,
                delay: timedelta, error: str) -> bool: ...
async def kill(self, task_id: int, lease_id: UUID, *, error: str) -> bool: ...
async def heartbeat(self, task_id: int, lease_id: UUID, extend_s: int) -> bool: ...
async def list_dead(self, queue: str, *, limit: int = 100,
                    before_id: Optional[int] = None) -> list[Task]: ...
async def requeue_dead(self, task_id: int, *,
                       payload: Optional[dict] = None,
                       max_attempts: Optional[int] = None,
                       delay: Optional[timedelta] = None) -> bool: ...
async def purge_dead(self, task_id: int) -> bool: ...
async def reap_completed(self, older_than: timedelta) -> int: ...
async def stats(self, queue: str) -> dict: ...
```

All worker-facing calls (ack, retry, kill, heartbeat) return `bool` — `True` if the lease
was still valid, `False` if it had been superseded.

`stats()` returns the dict served by `GET /queues/{queue}/stats`. Suggested shape:

```python
{
  "queue": "default",
  "depth": {"PENDING": 12, "RUNNING": 3, "SUCCEEDED": 9418, "DEAD": 4},
  "oldest_pending_age_s": 7.4,
}
```

---

## 9. Errors

Defined in `src/taskq/errors.py`:

- `RetriableError` — raised by handlers when the failure may succeed on retry.
- `FatalError` — raised by handlers when retrying cannot help; goes straight to DEAD.
- Any other exception thrown by a handler is treated as retriable (§6).
- Pydantic `ValidationError` raised during payload parsing is treated as `FatalError`
  (bad payload, retrying won't fix it; §21).

---

## 10. Retry policy

### 10.1 Backoff (§7)

```python
BASE_DELAY_S = 1.0
MAX_DELAY_S  = 600.0
MAX_SHIFT    = 10

def compute_backoff(attempts: int) -> float:
    backoff = BASE_DELAY_S * (2 ** min(attempts, MAX_SHIFT))
    jitter  = random.uniform(0, backoff / 2)
    return min(backoff + jitter, MAX_DELAY_S)
```

### 10.2 Per-status delay (§19.3)

```text
5xx response                : compute_backoff(attempts)
429 with Retry-After        : min(MAX_DELAY_S, max(compute_backoff(attempts), Retry-After))
429 without Retry-After     : compute_backoff(attempts)
408, 425                    : compute_backoff(attempts)
network error / timeout     : compute_backoff(attempts)
other 4xx                   : FatalError → DEAD (no retry)
```

`Retry-After` is **clamped** to `MAX_DELAY_S` (600s). Every retry — including 429 —
counts toward `max_attempts`.

### 10.3 `attempts` semantics (§19.7) — IMPORTANT

`attempts` is incremented **only** in the dispatch UPDATE (§5). `retry()` does NOT
re-increment. Concretely:

```text
fresh PENDING claimed              attempts += 1   (in dispatch UPDATE)
stale RUNNING reclaimed            attempts += 1   (in dispatch UPDATE)
handler raises → retry()           no further increment
handler raises → kill() / DLQ      no further increment
```

### 10.4 DLQ transition rule

When `Broker.retry()` is invoked, the row's `attempts` already reflects the just-claimed
attempt. If `attempts >= max_attempts`, **`retry()` flips status to `DEAD`** instead of
back to `PENDING`. The worker doesn't know or need to know this — it just calls
`broker.retry(...)`.

---

## 11. Worker lifecycle (§6)

```python
async def worker_loop(worker_id: str, queue: str):
    while not shutdown_event.is_set():
        task = await broker.claim(queue, worker_id)
        if task is None:
            await asyncio.sleep(poll_interval + random.random() * poll_interval)  # §19.6
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
                               delay=compute_backoff(task.attempts), error="task timeout")
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

Four invariants:

1. **Lease checks on every state-changing call.** ack, retry, kill, heartbeat all take
   `(task_id, lease_id)` and update only if the row's `lease_id` still matches.
2. **Heartbeats for long tasks.** Background coroutine extends `visible_until` every
   `timeout_s / 3` seconds. If heartbeat fails the lease check, cancel the task — the
   claim is gone.
3. **Per-task execution timeout.** `asyncio.wait_for(handler(payload), timeout=task.timeout_s)`
   — without it, heartbeats would extend a hung handler forever.
4. **Graceful shutdown.** On `SIGTERM`, set `shutdown_event`, stop claiming new tasks,
   await in-flight ones, then exit.

Worker process model in Docker (§20.2): one OS process per container replica = one
asyncio loop = `TASKQ_CONCURRENCY` coroutines. Scale via `docker compose up --scale worker=N`.

---

## 12. Admin endpoints (§14, §20.1)

JSON only — no HTML in Phase 3.

```text
GET  /health                                          (no auth)
GET  /queues/{queue}/stats                            (auth)
GET  /queues/{queue}/dead?limit=&before_id=           (auth)
POST /tasks/{task_id}/requeue                         (auth, optional JSON body)
POST /tasks/{task_id}/purge                           (auth)
```

Auth (§20.1): bearer token via `TASKQ_ADMIN_TOKEN`. Rule:

```text
TASKQ_ENV=prod, token unset      → process refuses to start
TASKQ_ENV=dev,  token unset      → auth disabled, loud warning at startup
token set                        → enforced everywhere except /health
```

Requeue body (all fields optional):

```json
{
  "payload":      { "...": "edited" },
  "max_attempts": 10,
  "delay_s":      30
}
```

The `reap_completed` background task runs in the API service (§20.5):

```text
TASKQ_REAP_ENABLED=true, every TASKQ_REAP_INTERVAL_S=300 seconds,
LIMIT TASKQ_REAP_BATCH=1000 per sweep,
older_than = TASKQ_REAP_RETENTION_DAYS days,
DEAD tasks are NEVER reaped automatically.
```

`TASKQ_REAP_ENABLED=false` for tests.

---

## 13. CLI commands (§20.4)

```text
taskq enqueue --queue Q --type T --payload JSON [--idempotency-key K] [--delay-s N] [--timeout-s N] [--max-attempts N]
taskq webhook send --url URL --event-type T --data JSON
taskq dlq list --queue Q [--limit N] [--before-id ID]
taskq dlq requeue TASK_ID [--payload JSON] [--max-attempts N] [--delay-s N]
taskq dlq purge TASK_ID
taskq stats --queue Q
```

`taskq webhook send` is the recorded-demo command. The other DLQ/stats commands are thin
wrappers over the admin endpoints (or the broker directly — implementer's choice).

---

## 14. Webhook contracts

### 14.1 Producer-facing API (§19.5)

```http
POST /events
Content-Type: application/json

{
  "event_type": "user.created",
  "data": { "user_id": "u_123", "email": "alice@example.com" }
}
```

The API creates one event row, looks up subscriptions for `event_type`, and enqueues one
delivery task per subscription.

### 14.2 Internal queue task payload (§19.5)

```json
{
  "delivery_id": "del_xxx",
  "subscription_id": "sub_xxx",
  "event_id": "evt_xxx",
  "target_url": "http://subscriber:9000/webhook",
  "event_type": "user.created",
  "body": { "event_id": "evt_xxx", "type": "user.created", "data": { "...": "..." } }
}
```

### 14.3 Worker POST headers (§19.5)

```http
POST {target_url}
Content-Type: application/json
X-Webhook-Id:        del_xxx
X-Webhook-Event-Id:  evt_xxx
X-Webhook-Timestamp: 2026-04-29T...Z
X-Webhook-Signature: sha256=<hmac(secret, timestamp + "." + body)>
Idempotency-Key:     del_xxx
```

`Idempotency-Key` equals `delivery_id`. Signing: HMAC-SHA256 over `timestamp + "." + body`,
secret from `TASKQ_WEBHOOK_SIGNING_SECRET` (§21).

httpx config (§21): `connect=5s`, `read=25s`, total budget ≤ `task.timeout_s − 5s`.
**One shared `httpx.AsyncClient` per process** — do not construct one per task.

### 14.4 Demo subscriber (§19.4)

```text
POST /webhook                  receiver under test
POST /admin/configure          set fail_next_n_requests, force_status, fail_rate
POST /admin/reset              clear all state — tests call this in setup
GET  /admin/deliveries         inspect what was received
```

State keyed by `delivery_id`. In-memory state is acceptable. Tests prefer
`force_status` / `fail_next_n_requests` over `FAIL_RATE` to avoid flakes.

### 14.5 Retry-After clamp

When subscriber returns `429` with `Retry-After`:

```python
delay = min(MAX_DELAY_S, max(compute_backoff(attempts), retry_after_seconds))
```

`Retry-After: 86400` must NOT pin a worker slot for a day — the clamp to `MAX_DELAY_S`
prevents this. If the receiver really needs longer, the task can DLQ and a human decides.

---

## 15. Test plan

### 15.1 Unit (no Docker, `pytest -m "not integration"`)

| Test                                    | Owner             |
|-----------------------------------------|-------------------|
| `compute_backoff` — bounds, jitter, cap | Worker + Webhook  |
| Handler registry — register / lookup / collision | Worker + Webhook |
| Payload size validation in `enqueue`    | Queue Core        |
| `Task.from_record` mapping              | Lead Architect    |
| `Settings` env-var loading + defaults   | Lead Architect (light) |

### 15.2 Integration (Testcontainers Postgres, `pytest -m integration`)

| Test                                                              | Owner             |
|-------------------------------------------------------------------|-------------------|
| enqueue → claim → ack happy path                                  | Queue Core        |
| visibility timeout reclaim after simulated worker death           | Queue Core        |
| **stale lease rejection** (A claims, B reclaims, A's ack returns False) | Queue Core  |
| DLQ transition at `max_attempts`                                  | Queue Core        |
| Concurrent SKIP LOCKED with `asyncio.gather`                      | Queue Core        |
| Producer-side idempotency (same key → same task)                  | Queue Core        |
| Worker happy path (loop runs, handler executes, ack happens)      | Worker + Webhook  |
| Handler timeout (`asyncio.wait_for` kills hung handler, retried)  | Worker + Webhook  |
| Webhook delivery + Retry-After honoring + force_status            | Worker + Webhook  |
| Admin endpoints: stats, dead list, requeue, purge, auth           | API + CLI         |
| `reap_completed` deletes SUCCEEDED ≥ N days, leaves DEAD alone    | Queue Core (SQL) + API+CLI (scheduler) |

### 15.3 Chaos (`pytest -m chaos`)

| Test                                                              | Owner             |
|-------------------------------------------------------------------|-------------------|
| 5 workers + 10k tasks + random `SIGKILL` every few seconds; assert no task lost, every task terminal, no duplicate side effects | Testing + DevEx |

Conftest fixtures (Testing + DevEx own):

- session-scoped Postgres container (Testcontainers).
- per-test `asyncpg` pool fixture.
- `truncate tasks` between tests rather than restarting Postgres.

---

## 16. Coordination protocol

- Each agent edits ONLY their owned files (§2 table). Reading teammate WIP is fine.
- If a teammate's interface needs to change (e.g. Worker realises `Broker.claim` should
  also return `X`), the proposing agent:
  1. Updates `TEAM_PLAN.md` (§8 in particular) with the new contract.
  2. Broadcasts the change in their final summary so the affected agent sees it next run.
  3. Does NOT edit the teammate's file directly.
- Confusion resolution order:
  1. Re-read the spec (`task-queue-service (3).md`).
  2. Re-read this `TEAM_PLAN.md`.
  3. Search the internet (asyncpg/Alembic/pydantic-settings docs).
  4. Flag the question in the agent's final summary so the user can decide.
  - There is NO live cross-agent chat in this setup.
- No commits during scaffolding. The user will commit manually.

---

## 17. Open questions

None at scaffold time — every ambiguity was resolved by spec §19–§21. If implementers
hit a genuine ambiguity, they should document it in their final summary so the user can
arbitrate.
