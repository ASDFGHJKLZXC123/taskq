from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
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
    idempotency_key: Optional[str]
    locked_by: Optional[str]
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime]
    visible_until: Optional[datetime]

    @classmethod
    def from_record(cls, record: Any) -> "Task":
        return cls(
            id=record["id"],
            queue=record["queue"],
            task_type=record["task_type"],
            payload=record["payload"],
            status=record["status"],
            attempts=record["attempts"],
            max_attempts=record["max_attempts"],
            available_at=record["available_at"],
            lease_id=record["lease_id"],
            timeout_s=record["timeout_s"],
            last_error=record["last_error"],
            idempotency_key=record["idempotency_key"],
            locked_by=record["locked_by"],
            created_at=record["created_at"],
            updated_at=record["updated_at"],
            completed_at=record["completed_at"],
            visible_until=record["visible_until"],
        )
