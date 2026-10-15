"""Обработка событий — без Kafka внутри.

Намеренное разделение: всё, что можно проверить без брокера, живёт здесь,
а `consumer.py` только качает сообщения и вызывает эти функции.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from uuid import UUID

from .events import DecodeError, EventType, TaskEvent, decode


@dataclass
class Outcome:
    """Итог разбора одного сообщения."""

    event: TaskEvent | None = None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.event is not None


@dataclass
class TaskProjection:
    """Проекция состояния задач, собранная из потока событий.

    Дедуплицирует по event_id: доставка в Kafka at-least-once, одно и то же
    сообщение может прийти дважды после ребаланса.
    """

    statuses: dict[UUID, str] = field(default_factory=dict)
    seen: set[UUID] = field(default_factory=set)

    def apply(self, event: TaskEvent) -> bool:
        """Применяет событие. False — если оно уже применялось."""
        if event.event_id in self.seen:
            return False
        self.seen.add(event.event_id)

        if event.type is EventType.TASK_DELETED:
            self.statuses.pop(event.task_id, None)
        elif event.type is EventType.TASK_CREATED:
            self.statuses[event.task_id] = event.payload.get("status", "todo")
        elif event.type is EventType.TASK_STATUS_CHANGED:
            new_status = event.payload.get("status")
            if new_status:
                self.statuses[event.task_id] = new_status
        return True


def parse(raw: bytes) -> Outcome:
    """Разбирает сырое сообщение, не поднимая исключений наружу.

    Битое сообщение — это не сбой консьюмера: такое уходит в DLQ, а чтение
    продолжается. Иначе одно кривое сообщение останавливает всю партицию.
    """
    try:
        return Outcome(event=decode(raw))
    except DecodeError as exc:
        return Outcome(error=str(exc))
