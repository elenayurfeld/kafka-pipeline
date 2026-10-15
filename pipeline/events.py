"""Схема события и правила (де)сериализации.

Контракт один для продюсера и консьюмера, поэтому лежит отдельным модулем
и не тянет за собой aiokafka — его можно импортировать в тестах без брокера.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from enum import Enum
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, ValidationError

SCHEMA_VERSION = 1


class EventType(str, Enum):
    TASK_CREATED = "task.created"
    TASK_STATUS_CHANGED = "task.status_changed"
    TASK_DELETED = "task.deleted"


class TaskEvent(BaseModel):
    """Событие жизненного цикла задачи.

    `schema_version` едет в каждом сообщении: консьюмер старой версии должен
    уметь внятно отказаться от незнакомого формата, а не упасть на разборе.
    """

    event_id: UUID = Field(default_factory=uuid4)
    schema_version: int = SCHEMA_VERSION
    type: EventType
    task_id: UUID
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    payload: dict[str, str] = Field(default_factory=dict)

    def key(self) -> bytes:
        """Ключ партиционирования — task_id.

        События одной задачи обязаны лечь в одну партицию, иначе порядок
        смены статусов не гарантирован.
        """
        return str(self.task_id).encode()

    def encode(self) -> bytes:
        return self.model_dump_json().encode()


class DecodeError(ValueError):
    """Сообщение не разобралось или не прошло схему."""


def decode(raw: bytes) -> TaskEvent:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise DecodeError(f"не JSON: {exc}") from exc

    if not isinstance(data, dict):
        raise DecodeError("ожидался объект")

    version = data.get("schema_version")
    if version != SCHEMA_VERSION:
        raise DecodeError(f"несовместимая версия схемы: {version!r}")

    try:
        return TaskEvent.model_validate(data)
    except ValidationError as exc:
        raise DecodeError(f"не прошло схему: {exc.error_count()} ошибок") from exc
