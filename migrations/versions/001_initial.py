"""initial schema

Revision ID: 001_initial
Revises:
Create Date: 2026-04-29

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "001_initial"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(sa.text("""
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
)
"""))
    op.execute(sa.text("""
CREATE INDEX idx_tasks_dispatch
    ON tasks (queue, available_at)
    WHERE status IN ('PENDING', 'RUNNING')
"""))
    op.execute(sa.text("""
CREATE UNIQUE INDEX idx_tasks_idempotency
    ON tasks (queue, idempotency_key)
    WHERE idempotency_key IS NOT NULL
"""))
    op.execute(sa.text("""
CREATE INDEX idx_tasks_completed_at
    ON tasks (completed_at)
    WHERE status IN ('SUCCEEDED', 'DEAD')
"""))


def downgrade() -> None:
    op.execute(sa.text("DROP INDEX IF EXISTS idx_tasks_completed_at"))
    op.execute(sa.text("DROP INDEX IF EXISTS idx_tasks_idempotency"))
    op.execute(sa.text("DROP INDEX IF EXISTS idx_tasks_dispatch"))
    op.execute(sa.text("DROP TABLE IF EXISTS tasks"))
