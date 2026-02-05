import asyncio
import json
import logging

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from orchestrator.kafka.config import kafka_settings

logger = logging.getLogger(__name__)


class KafkaCommandProducer:
    def __init__(self, *, topic: str, client_id: str):
        self._producer = AIOKafkaProducer(
            bootstrap_servers=kafka_settings.bootstrap_servers,
            client_id=client_id,
            enable_idempotence=True,
            acks="all",
        )
        self._topic = topic
        self._started = False
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        async with self._lock:
            if self._started:
                return
            await self._producer.start()
            self._started = True
            logger.info("Kafka producer started")

    async def stop(self) -> None:
        async with self._lock:
            if not self._started:
                return
            await self._producer.stop()
            self._started = False
            logger.info("Kafka producer stopped")

    async def publish_event(self, event: dict) -> bool:
        if not self._started:
            raise RuntimeError("Kafka producer is not started")
        try:
            await self._producer.send_and_wait(
                topic=self._topic,
                key=event["order_id"].encode("utf-8"),
                value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
            )
            logger.debug("order_id=%s message_id=%s published", event.get("order_id"), event.get("message_id"))
            return True
        except KafkaError:
            logger.exception("Kafka publish failed for order %s", event.get("order_id"))
            return False
        except Exception:
            logger.exception("Unexpected error while publishing order %s", event.get("order_id"))
            return False

_inventory_producer: KafkaCommandProducer | None = None
_payment_producer: KafkaCommandProducer | None = None

def get_inventory_producer() -> KafkaCommandProducer:
    global _inventory_producer
    if _inventory_producer is None:
        _inventory_producer = KafkaCommandProducer(
            topic=kafka_settings.inventory_commands_topic,
            client_id="orchestrator-inventory",
        )
    return _inventory_producer

def get_payment_producer() -> KafkaCommandProducer:
    global _payment_producer
    if _payment_producer is None:
        _payment_producer = KafkaCommandProducer(
            topic=kafka_settings.payment_commands_topic,
            client_id="orchestrator-payment",
        )
    return _payment_producer

_order_events_producer: KafkaCommandProducer | None = None
_notifications_producer: KafkaCommandProducer | None = None

def get_order_events_producer() -> KafkaCommandProducer:
  global _order_events_producer
  if _order_events_producer is None:
      _order_events_producer = KafkaCommandProducer(
          topic=kafka_settings.orders_topic,
          client_id="orchestrator-orders-producer",
      )
  return _order_events_producer

def get_notifications_producer() -> KafkaCommandProducer:
  global _notifications_producer
  if _notifications_producer is None:
      _notifications_producer = KafkaCommandProducer(
          topic=kafka_settings.notifications_topic,
          client_id="orchestrator-notifications",
      )
  return _notifications_producer