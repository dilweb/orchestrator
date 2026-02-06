import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from orchestrator.kafka.config import kafka_settings

logger = logging.getLogger(__name__)

OrderEventHandler = Callable[[dict[str, Any]], Awaitable[None]]


class OrderKafkaConsumer:
    def __init__(
        self,
        *,
        bootstrap_servers: str | None = None,
        topic: str | None = None,
        group_id: str | None = None,
    ) -> None:
        self._bootstrap = bootstrap_servers or kafka_settings.bootstrap_servers
        self._topic = topic or kafka_settings.orders_topic
        self._group_id = group_id or kafka_settings.orders_consumer_group
        self._consumer = AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=self._bootstrap,
            group_id=self._group_id,
            enable_auto_commit=True,
            auto_offset_reset="earliest",
        )
        self._started = False
        self._lock = asyncio.Lock()


    async def __aenter__(self) -> "OrderKafkaConsumer":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.stop()

    async def start(self) -> None:
        async with self._lock:
            if self._started:
                return
            await self._consumer.start()
            self._started = True
            logger.info(
                "Kafka consumer started (topic=%s, group=%s)", self._topic, self._group_id
            )

    async def stop(self) -> None:
        async with self._lock:
            if not self._started:
                return
            await self._consumer.stop()
            self._started = False
            logger.info("Kafka consumer stopped (group=%s)", self._group_id)

    async def consume(self, handler: OrderEventHandler) -> None:
        if not self._started:
            raise RuntimeError("Kafka consumer is not started")
        try:
            async for message in self._consumer:
                payload = self._deserialize(message.value)
                try:
                    await handler(payload)
                except Exception:
                    logger.exception(
                        "Consumer handler failed for order_id=%s", payload.get("order_id")
                    )
        except asyncio.CancelledError:
            raise
        except KafkaError:
            logger.exception("Kafka consumer error")
        except Exception:
            logger.exception("Unexpected consumer failure")

    @staticmethod
    def _deserialize(value: bytes) -> dict[str, Any]:
        if not value:
            return {}
        return json.loads(value.decode("utf-8"))
