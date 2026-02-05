import logging
import os

from pydantic_settings import BaseSettings


class KafkaSettings(BaseSettings):
    bootstrap_servers: str = "kafka:29092"

    orders_topic: str = "order.events"
    orders_consumer_group: str = "orchestrator-orders"

    inventory_commands_topic: str = "inventory.commands"
    payment_commands_topic: str = "payment.commands"
    notifications_topic: str = "notifications"
    dlq_topic: str = "retry.events"

    max_retry_attempts: int = 5
    retry_window_seconds: int = 10
    dedup_ttl_seconds: int = 300

    class Config:
        env_prefix = "KAFKA_"

kafka_settings = KafkaSettings()
