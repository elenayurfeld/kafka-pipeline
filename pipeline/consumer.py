"""Консьюмер с ручным коммитом и отводом битых сообщений в DLQ.

Запуск: `python -m pipeline.consumer`
"""

from __future__ import annotations

import asyncio
import logging

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from .config import settings
from .processing import TaskProjection, parse

log = logging.getLogger("consumer")

projection = TaskProjection()


async def run() -> None:
    consumer = AIOKafkaConsumer(
        settings.topic,
        bootstrap_servers=settings.bootstrap_servers,
        group_id=settings.group_id,
        # Коммитим сами, после обработки: автокоммит теряет сообщения при падении.
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=settings.max_poll_records,
    )
    dlq = AIOKafkaProducer(bootstrap_servers=settings.bootstrap_servers, acks="all")

    await consumer.start()
    await dlq.start()
    try:
        async for record in consumer:
            outcome = parse(record.value)
            if outcome.ok:
                assert outcome.event is not None
                applied = projection.apply(outcome.event)
                log.info(
                    "%s %s%s",
                    outcome.event.type.value,
                    outcome.event.task_id,
                    "" if applied else " (дубль, пропущен)",
                )
            else:
                # Битое сообщение не должно блокировать партицию — уводим в DLQ.
                log.warning("в DLQ: %s", outcome.error)
                await dlq.send_and_wait(
                    settings.dlq_topic, value=record.value, key=record.key
                )
            await consumer.commit()
    finally:
        await dlq.stop()
        await consumer.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\nостановлено")


if __name__ == "__main__":
    main()
