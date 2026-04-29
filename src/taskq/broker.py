from __future__ import annotations

import json
from datetime import timedelta
from typing import Optional
from uuid import UUID

import asyncpg

from taskq.models import Task
from taskq.settings import Settings, get_settings


class Broker:
    def __init__(self, pool: asyncpg.Pool, settings: Optional[Settings] = None) -> None:
        self._pool = pool
        self._settings = settings or get_settings()

    async def enqueue(
        self,
        queue: str,
        task_type: str,
        payload: dict,
        *,
        idempotency_key: Optional[str] = None,
        delay: Optional[timedelta] = None,
        timeout_s: int = 60,
        max_attempts: int = 5,
    ) -> int:
        encoded = json.dumps(payload, default=str).encode("utf-8")
        if len(encoded) > self._settings.TASKQ_PAYLOAD_MAX_BYTES:
            raise ValueError("payload exceeds TASKQ_PAYLOAD_MAX_BYTES")

        delay_s = delay.total_seconds() if delay is not None else 0.0

        sql = """
            INSERT INTO tasks (
                queue, task_type, payload, status, max_attempts,
                available_at, idempotency_key, timeout_s
            )
            VALUES (
                $1, $2, $3::jsonb, 'PENDING', $4,
                NOW() + make_interval(secs => $5), $6, $7
            )
            ON CONFLICT (queue, idempotency_key)
            WHERE idempotency_key IS NOT NULL
            DO NOTHING
            RETURNING id
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                sql,
                queue,
                task_type,
                payload,
                max_attempts,
                delay_s,
                idempotency_key,
                timeout_s,
            )
            if row is not None:
                return row["id"]

            existing = await conn.fetchrow(
                "SELECT id FROM tasks WHERE queue = $1 AND idempotency_key = $2",
                queue,
                idempotency_key,
            )
            if existing is None:
                raise RuntimeError("enqueue produced no row and no existing task found")
            return existing["id"]

    async def claim(self, queue: str, worker_id: str) -> Optional[Task]:
        sql = """
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
            RETURNING *
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, worker_id, queue)
            if row is None:
                return None
            return Task.from_record(row)

    async def ack(self, task_id: int, lease_id: UUID) -> bool:
        sql = """
            UPDATE tasks
            SET status = 'SUCCEEDED', completed_at = NOW(), updated_at = NOW(),
                locked_by = NULL, lease_id = NULL
            WHERE id = $1 AND lease_id = $2
            RETURNING id
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, task_id, lease_id)
            return row is not None

    async def retry(
        self,
        task_id: int,
        lease_id: UUID,
        *,
        delay: timedelta,
        error: str,
    ) -> bool:
        delay_s = delay.total_seconds()
        sql = """
            UPDATE tasks
            SET status = CASE
                    WHEN attempts >= max_attempts THEN 'DEAD'
                    ELSE 'PENDING'
                END,
                available_at = CASE
                    WHEN attempts >= max_attempts THEN available_at
                    ELSE NOW() + make_interval(secs => $3)
                END,
                visible_until = NULL,
                locked_by = NULL,
                lease_id = NULL,
                last_error = $4,
                completed_at = CASE
                    WHEN attempts >= max_attempts THEN NOW()
                    ELSE NULL
                END,
                updated_at = NOW()
            WHERE id = $1 AND lease_id = $2 AND status = 'RUNNING'
            RETURNING id
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, task_id, lease_id, delay_s, error)
            return row is not None

    async def kill(self, task_id: int, lease_id: UUID, *, error: str) -> bool:
        sql = """
            UPDATE tasks
            SET status = 'DEAD',
                last_error = $3,
                completed_at = NOW(),
                locked_by = NULL,
                lease_id = NULL,
                updated_at = NOW()
            WHERE id = $1 AND lease_id = $2
            RETURNING id
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, task_id, lease_id, error)
            return row is not None

    async def heartbeat(self, task_id: int, lease_id: UUID, extend_s: int) -> bool:
        sql = """
            UPDATE tasks
            SET visible_until = NOW() + make_interval(secs => $3),
                updated_at = NOW()
            WHERE id = $1
              AND lease_id = $2
              AND status = 'RUNNING'
            RETURNING id
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, task_id, lease_id, extend_s)
            return row is not None

    async def list_dead(
        self,
        queue: str,
        *,
        limit: int = 100,
        before_id: Optional[int] = None,
    ) -> list[Task]:
        sql = """
            SELECT * FROM tasks
            WHERE status = 'DEAD'
              AND queue = $1
              AND ($2::bigint IS NULL OR id < $2)
            ORDER BY id DESC
            LIMIT $3
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, queue, before_id, limit)
            return [Task.from_record(r) for r in rows]

    async def requeue_dead(
        self,
        task_id: int,
        *,
        payload: Optional[dict] = None,
        max_attempts: Optional[int] = None,
        delay: Optional[timedelta] = None,
    ) -> bool:
        delay_s = delay.total_seconds() if delay is not None else None

        sql = """
            UPDATE tasks
            SET status        = 'PENDING',
                attempts      = 0,
                available_at  = COALESCE(NOW() + make_interval(secs => $4::double precision), NOW()),
                visible_until = NULL,
                locked_by     = NULL,
                lease_id      = NULL,
                last_error    = NULL,
                completed_at  = NULL,
                payload       = COALESCE($2::jsonb, payload),
                max_attempts  = COALESCE($3, max_attempts),
                updated_at    = NOW()
            WHERE id = $1
              AND status = 'DEAD'
            RETURNING id
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                sql,
                task_id,
                payload,
                max_attempts,
                delay_s,
            )
            return row is not None

    async def purge_dead(self, task_id: int) -> bool:
        sql = "DELETE FROM tasks WHERE id = $1 AND status = 'DEAD' RETURNING id"
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, task_id)
            return row is not None

    async def reap_completed(self, older_than: timedelta) -> int:
        older_than_s = older_than.total_seconds()
        batch = self._settings.TASKQ_REAP_BATCH
        sql = """
            DELETE FROM tasks
            WHERE id IN (
                SELECT id FROM tasks
                WHERE status = 'SUCCEEDED'
                  AND completed_at < NOW() - make_interval(secs => $1)
                LIMIT $2
            )
        """
        total = 0
        async with self._pool.acquire() as conn:
            while True:
                result = await conn.execute(sql, older_than_s, batch)
                # result is like 'DELETE 17'
                deleted = int(result.split()[-1]) if result else 0
                total += deleted
                if deleted == 0:
                    break
        return total

    async def stats(self, queue: str) -> dict:
        sql = """
            SELECT status,
                   COUNT(*)::bigint AS depth,
                   EXTRACT(EPOCH FROM (NOW() - MIN(available_at) FILTER (WHERE status = 'PENDING'))) AS oldest_pending_age
            FROM tasks
            WHERE queue = $1
            GROUP BY status
        """
        depth_by_status: dict[str, int] = {
            "PENDING": 0,
            "RUNNING": 0,
            "SUCCEEDED": 0,
            "DEAD": 0,
        }
        oldest_age: Optional[float] = None
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, queue)
            for r in rows:
                status = r["status"]
                if status in depth_by_status:
                    depth_by_status[status] = int(r["depth"])
                if status == "PENDING" and r["oldest_pending_age"] is not None:
                    oldest_age = float(r["oldest_pending_age"])

        return {
            "depth_by_status": depth_by_status,
            "oldest_pending_age_seconds": oldest_age,
        }
