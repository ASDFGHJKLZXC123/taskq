"""Unit tests for taskq.models.Task — round-trip via ``Task.from_record``.

Use a plain dict instead of a real asyncpg.Record because Records can't be
constructed in user code. ``Task.from_record`` accepts any mapping (it just
indexes by string).
"""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from taskq.models import Task


def _record_dict(**overrides):
    base = {
        "id": 42,
        "queue": "default",
        "task_type": "noop",
        "payload": {"x": 1, "y": "two"},
        "status": "PENDING",
        "attempts": 0,
        "max_attempts": 5,
        "available_at": datetime(2026, 4, 29, 12, 0, 0, tzinfo=timezone.utc),
        "lease_id": None,
        "timeout_s": 60,
        "last_error": None,
        "idempotency_key": None,
        "locked_by": None,
        "created_at": datetime(2026, 4, 29, 11, 59, 0, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 4, 29, 11, 59, 30, tzinfo=timezone.utc),
        "completed_at": None,
        "visible_until": None,
    }
    base.update(overrides)
    return base


def test_from_record_pending() -> None:
    rec = _record_dict()
    t = Task.from_record(rec)
    assert t.id == 42
    assert t.queue == "default"
    assert t.task_type == "noop"
    assert t.payload == {"x": 1, "y": "two"}
    assert t.status == "PENDING"
    assert t.attempts == 0
    assert t.max_attempts == 5
    assert t.lease_id is None
    assert t.timeout_s == 60
    assert t.last_error is None
    assert t.idempotency_key is None
    assert t.locked_by is None
    assert t.completed_at is None
    assert t.visible_until is None


def test_from_record_running_with_lease() -> None:
    lease = uuid4()
    visible = datetime(2026, 4, 29, 12, 1, 0, tzinfo=timezone.utc)
    rec = _record_dict(
        status="RUNNING",
        attempts=1,
        lease_id=lease,
        locked_by="worker-A",
        visible_until=visible,
    )
    t = Task.from_record(rec)
    assert t.status == "RUNNING"
    assert t.attempts == 1
    assert t.lease_id == lease
    assert t.locked_by == "worker-A"
    assert t.visible_until == visible


def test_from_record_dead_with_error() -> None:
    completed = datetime(2026, 4, 29, 12, 5, 0, tzinfo=timezone.utc)
    rec = _record_dict(
        status="DEAD",
        attempts=5,
        max_attempts=5,
        last_error="went bad",
        completed_at=completed,
        idempotency_key="evt-123",
    )
    t = Task.from_record(rec)
    assert t.status == "DEAD"
    assert t.last_error == "went bad"
    assert t.completed_at == completed
    assert t.idempotency_key == "evt-123"


def test_from_record_payload_preserves_nested_dict() -> None:
    payload = {
        "delivery_id": "del_1",
        "body": {"event_id": "evt_1", "type": "user.created", "data": {"id": 1}},
    }
    rec = _record_dict(payload=payload)
    t = Task.from_record(rec)
    assert t.payload == payload
    # Same dict reference — broker is allowed to share the JSONB-decoded object.
    assert t.payload["body"]["data"] == {"id": 1}


def test_from_record_works_with_mapping_subclass() -> None:
    class MappingShim(dict):
        pass

    rec = MappingShim(_record_dict())
    t = Task.from_record(rec)
    assert t.id == 42
    assert t.queue == "default"
