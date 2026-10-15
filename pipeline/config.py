"""Настройки из переменных окружения. Дефолты рассчитаны на docker-compose рядом."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topic: str = os.getenv("KAFKA_TOPIC", "task-events")
    dlq_topic: str = os.getenv("KAFKA_DLQ_TOPIC", "task-events.dlq")
    group_id: str = os.getenv("KAFKA_GROUP_ID", "task-events-worker")
    # Коммит только после успешной обработки, поэтому автокоммит выключен.
    max_poll_records: int = int(os.getenv("KAFKA_MAX_POLL_RECORDS", "50"))


settings = Settings()
