# taskq — a Postgres-backed task queue

## What this is

`taskq` is a Postgres-backed task queue with distributed workers, retries with
exponential backoff, idempotency keys, and a dead-letter queue. It ships with a
small webhook delivery demo: workers POST signed payloads to subscriber URLs,
honour `Retry-After`, and drop misbehaving tasks into the DLQ for human review.
It's a portfolio-shaped project for thinking about delivery guarantees,
fencing tokens, and crash recovery — not a Celery replacement.

## Architecture

```
┌────────────┐    enqueue    ┌──────────────┐   poll/claim   ┌────────────┐
│  Producer  │ ────────────▶ │              │ ◀───────────── │  Worker 1  │
│ (taskq CLI │               │              │   ack/nack     └────────────┘
│  or admin) │               │    Broker    │ ─────────────▶ ┌────────────┐
└────────────┘               │  (Postgres)  │                │  Worker 2  │
┌────────────┐    enqueue    │              │ ◀───────────── └────────────┘
│ Admin API  │ ────────────▶ │              │                ┌────────────┐
└────────────┘               └──────┬───────┘ ◀───────────── │  Worker N  │
                                    │                        └────────────┘
                                    ▼
                          status='DEAD' rows = the DLQ
```

A single Postgres table is the source of truth. Workers pull rows with
`SELECT ... FOR UPDATE SKIP LOCKED`. Heartbeats keep `visible_until` fresh; a
crashed worker's task is automatically reclaimed once that deadline passes.

See [`docs/SPEC.md`](./docs/SPEC.md) for the
spec and design discussion.

## Quickstart with Docker

```bash
# 1. Bring the stack up with two worker replicas to demo SKIP LOCKED.
docker compose up --scale worker=2 -d

# 2. Send a webhook from inside the api container.
docker compose exec api taskq webhook send \
    --url http://subscriber:9000/webhook \
    --event-type user.created \
    --data '{"id":1}' \
    --queue webhooks

# 3. Watch the queue drain.
docker compose exec api taskq stats --queue webhooks

# 4. Check what the subscriber received.
docker compose exec subscriber python -c \
  "import urllib.request,sys; sys.stdout.write(urllib.request.urlopen('http://localhost:9000/admin/deliveries').read().decode())"

# 5. Tear down.
docker compose down -v
```

Notes:

- `docker-compose.yml` declares `worker.deploy.replicas: 2`, so `docker compose up`
  alone spins up two workers. The `--scale worker=N` flag is the explicit
  override and is what you should reach for in demos.
- The `webhooks` queue name is the default the worker container listens on
  (`TASKQ_QUEUE=webhooks`). Override via `.env` if you want a different name.
- `.env` is required by the compose `env_file` directive — copy `.env.example`
  to `.env` first if you don't have one.

## Quickstart without Docker

You need Python 3.12+ and a reachable Postgres 16. Either start one yourself or
run `docker run -d --name taskq-pg -p 5432:5432 -e POSTGRES_USER=taskq -e POSTGRES_PASSWORD=taskq -e POSTGRES_DB=taskq postgres:16`.

```bash
python3.12 -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"

cp .env.example .env
export TASKQ_DATABASE_URL=postgresql://taskq:taskq@localhost:5432/taskq
alembic upgrade head
```

Run each component in its own terminal:

```bash
# Terminal 1 — admin API on :8000
taskq-admin

# Terminal 2 — worker process
TASKQ_QUEUE=webhooks taskq-worker

# Terminal 3 — demo subscriber on :9000
python -m taskq.subscriber.main
```

Then enqueue a delivery from a fourth terminal:

```bash
taskq webhook send \
    --url http://localhost:9000/webhook \
    --event-type user.created \
    --data '{"id":1}' \
    --queue webhooks

taskq stats --queue webhooks
```

## Demo flow

The recorded demo (§1 of the spec) walks through retries, the DLQ, and
requeue-with-edits.

```bash
# 0) Stack up.
docker compose up --scale worker=2 -d

# 1) Configure the subscriber to fail the next 2 requests with 503, then succeed.
docker compose exec subscriber python -c "
import urllib.request, json
req = urllib.request.Request(
    'http://localhost:9000/admin/configure',
    data=json.dumps({'fail_next_n_requests': 2, 'force_status': 503}).encode(),
    headers={'Content-Type': 'application/json'},
)
print(urllib.request.urlopen(req).read().decode())
"

# 2) Enqueue a delivery. The worker will retry with exponential backoff and
#    the third attempt will succeed.
docker compose exec api taskq webhook send \
    --url http://subscriber:9000/webhook \
    --event-type user.created \
    --data '{"id":1}' \
    --queue webhooks

# 3) Force a permanent 4xx so the next delivery goes straight to DEAD.
docker compose exec subscriber python -c "
import urllib.request, json
urllib.request.urlopen('http://localhost:9000/admin/reset')
req = urllib.request.Request(
    'http://localhost:9000/admin/configure',
    data=json.dumps({'fail_next_n_requests': 999, 'force_status': 400}).encode(),
    headers={'Content-Type': 'application/json'},
)
urllib.request.urlopen(req)
"

docker compose exec api taskq webhook send \
    --url http://subscriber:9000/webhook \
    --event-type user.created \
    --data '{"id":2}' \
    --queue webhooks

# 4) Inspect the DLQ.
docker compose exec api taskq dlq list --queue webhooks

# 5) Reset the subscriber and requeue the dead task with the original payload.
docker compose exec subscriber python -c "
import urllib.request
urllib.request.urlopen('http://localhost:9000/admin/reset')
"

# Replace TASK_ID with the id from `taskq dlq list`.
docker compose exec api taskq dlq requeue TASK_ID

# 6) Verify the subscriber received the redelivery.
docker compose exec subscriber python -c \
  "import urllib.request,sys; sys.stdout.write(urllib.request.urlopen('http://localhost:9000/admin/deliveries').read().decode())"
```

## Tests

```bash
# Fast suite — no Docker needed.
pytest -m "not integration"

# Full integration suite — Testcontainers spins up Postgres 16.
pytest -m integration

# Long-running chaos test (~35s wall time).
pytest -m chaos

# Opt-in compose smoke test (boots the full stack).
TASKQ_RUN_COMPOSE_TEST=1 pytest -m compose
```

The integration suite uses a session-scoped Postgres container and truncates
the `tasks` table between tests, so all integration tests share one container.

## Configuration

All runtime configuration is environment-driven via `pydantic-settings`. See
`.env.example` for the full list of variables and defaults.

## Project layout

```
src/taskq/
  broker.py           enqueue / claim / ack / retry / kill / heartbeat (asyncpg)
  worker.py           worker_loop, heartbeat_loop, signal handling
  admin.py            FastAPI admin app (stats, dead, requeue, purge)
  cli.py              click CLI: taskq, taskq-worker, taskq-admin
  handlers/webhook.py webhook delivery handler (HMAC-signed POST)
  subscriber/main.py  demo FastAPI subscriber (force_status, fail_next_n)
  models.py           Task dataclass + from_record
  settings.py         pydantic-settings configuration
migrations/           Alembic migrations (raw SQL)
tests/
  unit/               no-Docker unit tests
  integration/        Testcontainers + Postgres
docker-compose.yml    db, migrate, api, worker (replicas=2), subscriber
```

See [`TEAM_PLAN.md`](./TEAM_PLAN.md) for file ownership across the implementation team.

## Limitations / phase scope

This implementation covers Phases 1–3 of the spec: MVP enqueue/claim/ack,
lease IDs and visibility timeouts, retries with backoff and DLQ, producer-side
idempotency, JSON admin API, and the webhook delivery demo. Out of scope:
`LISTEN`/`NOTIFY` push dispatch, Prometheus metrics, an HTML admin dashboard,
priority queues beyond the `queue` parameter, and any benchmarking work
(Phases 4–6 in the spec).
