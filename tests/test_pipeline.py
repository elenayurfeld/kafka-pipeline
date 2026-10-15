"""Тесты контракта и обработки. Брокер не нужен: aiokafka здесь не импортируется."""

from __future__ import annotations

import json
from uuid import uuid4

from pipeline.events import SCHEMA_VERSION, EventType, TaskEvent, decode
from pipeline.processing import TaskProjection, parse


def test_roundtrip() -> None:
    event = TaskEvent(type=EventType.TASK_CREATED, task_id=uuid4(), payload={"status": "todo"})
    assert decode(event.encode()) == event


def test_key_is_task_id() -> None:
    task_id = uuid4()
    event = TaskEvent(type=EventType.TASK_DELETED, task_id=task_id)
    assert event.key() == str(task_id).encode()


def test_broken_json_goes_to_error_not_exception() -> None:
    outcome = parse("{не json".encode())
    assert not outcome.ok
    assert "не JSON" in (outcome.error or "")


def test_future_schema_version_rejected() -> None:
    raw = json.dumps(
        {
            "event_id": str(uuid4()),
            "schema_version": SCHEMA_VERSION + 1,
            "type": "task.created",
            "task_id": str(uuid4()),
            "occurred_at": "2026-10-15T00:00:00Z",
            "payload": {},
        }
    ).encode()
    outcome = parse(raw)
    assert not outcome.ok
    assert "версия схемы" in (outcome.error or "")


def test_unknown_event_type_rejected() -> None:
    raw = json.dumps(
        {
            "event_id": str(uuid4()),
            "schema_version": SCHEMA_VERSION,
            "type": "task.exploded",
            "task_id": str(uuid4()),
            "occurred_at": "2026-10-15T00:00:00Z",
            "payload": {},
        }
    ).encode()
    assert not parse(raw).ok


def test_projection_tracks_status() -> None:
    task_id = uuid4()
    projection = TaskProjection()

    projection.apply(TaskEvent(type=EventType.TASK_CREATED, task_id=task_id, payload={"status": "todo"}))
    projection.apply(
        TaskEvent(type=EventType.TASK_STATUS_CHANGED, task_id=task_id, payload={"status": "done"})
    )
    assert projection.statuses[task_id] == "done"

    projection.apply(TaskEvent(type=EventType.TASK_DELETED, task_id=task_id))
    assert task_id not in projection.statuses


def test_redelivered_event_applied_once() -> None:
    """Доставка at-least-once: повтор не должен менять проекцию."""
    task_id = uuid4()
    event = TaskEvent(type=EventType.TASK_CREATED, task_id=task_id, payload={"status": "todo"})
    projection = TaskProjection()

    assert projection.apply(event) is True
    assert projection.apply(event) is False
    assert len(projection.seen) == 1
