"""Продюсер событий задач.

Запуск как CLI: `python -m pipeline.producer --count 10`
"""

from __future__ import annotations

import argparse
import asyncio
import random
from uuid import uuid4

from aiokafka import AIOKafkaProducer

from .config import settings
from .events import EventType, TaskEvent


async def send(producer: AIOKafkaProducer, event: TaskEvent) -> None:
    # Ключ = task_id: порядок событий одной задачи сохраняется внутри партиции.
    await producer.send_and_wait(settings.topic, value=event.encode(), key=event.key())


async def produce(count: int) -> None:
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.bootstrap_servers,
        # acks=all + идемпотентность: без этого при ретрае возможны дубли и дыры.
        acks="all",
        enable_idempotence=True,
    )
    await producer.start()
    try:
        for _ in range(count):
            task_id = uuid4()
            await send(
                producer,
                TaskEvent(
                    type=EventType.TASK_CREATED,
                    task_id=task_id,
                    payload={"title": f"task-{task_id.hex[:6]}", "status": "todo"},
                ),
            )
            await send(
                producer,
                TaskEvent(
                    type=EventType.TASK_STATUS_CHANGED,
                    task_id=task_id,
                    payload={"status": random.choice(["in_progress", "done"])},
                ),
            )
            print(f"отправлено 2 события для {task_id}")
    finally:
        await producer.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Налить тестовых событий в топик")
    parser.add_argument("--count", type=int, default=5, help="сколько задач сгенерировать")
    args = parser.parse_args()
    asyncio.run(produce(args.count))


if __name__ == "__main__":
    main()
