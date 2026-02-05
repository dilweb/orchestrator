import asyncio
import logging
from time import monotonic
from typing import Any, Literal
from uuid import UUID, uuid4

from orchestrator.kafka.producer import KafkaCommandProducer
from orchestrator.models.messages import (
    InventoryReserveCommand,
    InventoryReserveFailed,
    InventoryReserved,
    InventoryReservationCancelled,
    OrderCancelled,
    OrderCompleted,
    PaymentChargeCommand,
    PaymentFailed,
    PaymentSucceeded,
)
from orchestrator.models.order import OrderEvent
from orchestrator.kafka.config import kafka_settings

logger = logging.getLogger(__name__)


class OrderSagaRunner:
    def __init__(
            self,
            *,
            inventory_producer: KafkaCommandProducer,
            payment_producer: KafkaCommandProducer,
            order_events_producer: KafkaCommandProducer,
            notifications_producer: KafkaCommandProducer,
    ) -> None:
        self._inventory = inventory_producer
        self._payment = payment_producer
        self._order_events = order_events_producer
        self._notifications = notifications_producer

        self._orders: dict[UUID, OrderEvent] = {}
        self._states: dict[UUID, Literal["created", "reserved"]] = {}
        self._processed_messages: dict[UUID, float] = {}
        self._dedup_ttl = kafka_settings.dedup_ttl_seconds

    def _seen(self, message_id: UUID) -> bool:
        now = monotonic()
        threshold = now - self._dedup_ttl
        expired = [mid for mid, ts in self._processed_messages.items() if ts < threshold]
        for mid in expired:
            self._processed_messages.pop(mid, None)

        if message_id in self._processed_messages:
            logger.debug("Duplicate message %s, skip", message_id)
            return True
        self._processed_messages[message_id] = now
        return False

    async def handle_order_created(self, event: dict[str, Any]) -> None:
        order = OrderEvent.model_validate(event)
        if self._seen(order.message_id):
            return
        self._orders[order.order_id] = order
        self._states[order.order_id] = "created"

        command = InventoryReserveCommand(
            order_id=order.order_id,
            saga_id=order.saga_id,
            message_id=uuid4(),
            correlation_id=order.message_id,
            items=order.payload.items,
        )
        await self._inventory.publish_event(command.model_dump(mode="json"))
        logger.info("Saga started order=%s saga=%s", order.order_id, order.saga_id)

    async def handle_inventory_reserved(self, event: dict[str, Any]) -> None:
        reserved = InventoryReserved.model_validate(event)
        if self._seen(reserved.message_id):
            return
        order = self._orders.get(reserved.order_id)
        if order is None:
            logger.warning("Unknown order for inventory.reserved order=%s", reserved.order_id)
            return
        if self._states.get(order.order_id) == "reserved":
            logger.debug("Order %s already reserved, skip duplicate", order.order_id)
            return

        self._states[order.order_id] = "reserved"
        command = PaymentChargeCommand(
            order_id=order.order_id,
            saga_id=order.saga_id,
            message_id=uuid4(),
            correlation_id=reserved.message_id,
            amount=str(order.payload.total),
            currency=order.payload.currency,
        )
        await self._payment.publish_event(command.model_dump(mode="json"))
        logger.info("Inventory reserved order=%s, charging payment", order.order_id)

    async def handle_inventory_reserve_failed(self, event: dict[str, Any]) -> None:
        failed = InventoryReserveFailed.model_validate(event)
        if self._seen(failed.message_id):
            return
        order = self._orders.pop(failed.order_id, None)
        self._states.pop(failed.order_id, None)
        if order is None:
            logger.warning("Unknown order for inventory.reserve-failed order=%s", failed.order_id)
            return

        cancellation = OrderCancelled(
            order_id=order.order_id,
            saga_id=order.saga_id,
            message_id=uuid4(),
            correlation_id=failed.message_id,
            reason=failed.reason,
        )
        await self._order_events.publish_event(cancellation.model_dump(mode="json"))
        await self._notifications.publish_event(
            {
                "event_type": "notification.order-cancelled",
                "order_id": str(order.order_id),
                "user_id": order.payload.user_id,
                "reason": failed.reason,
            }
        )
        logger.info("Order %s cancelled (inventory failure: %s)", order.order_id, failed.reason)

    async def handle_payment_succeeded(self, event: dict[str, Any]) -> None:
        succeeded = PaymentSucceeded.model_validate(event)
        if self._seen(succeeded.message_id):
            return
        order = self._orders.pop(succeeded.order_id, None)
        self._states.pop(succeeded.order_id, None)
        if order is None:
            logger.warning("Unknown order for payment.succeeded order=%s", succeeded.order_id)
            return

        completion = OrderCompleted(
            order_id=order.order_id,
            saga_id=order.saga_id,
            message_id=uuid4(),
            correlation_id=succeeded.message_id,
        )
        await self._order_events.publish_event(completion.model_dump(mode="json"))
        await self._notifications.publish_event(
            {
                "event_type": "notification.order-completed",
                "order_id": str(order.order_id),
                "user_id": order.payload.user_id,
            }
        )
        logger.info("Order %s completed successfully", order.order_id)

    async def handle_payment_failed(self, event: dict[str, Any]) -> None:
        failed = PaymentFailed.model_validate(event)
        if self._seen(failed.message_id):
            return
        order = self._orders.pop(failed.order_id, None)
        self._states.pop(failed.order_id, None)
        if order is None:
            logger.warning("Unknown order for payment.failed order=%s", failed.order_id)
            return

        release_cmd = InventoryReservationCancelled(
            order_id=order.order_id,
            saga_id=order.saga_id,
            message_id=uuid4(),
            correlation_id=failed.message_id,
            reason="payment_failed",
        )
        cancellation = OrderCancelled(
            order_id=order.order_id,
            saga_id=order.saga_id,
            message_id=uuid4(),
            correlation_id=failed.message_id,
            reason=failed.reason,
        )

        await self._inventory.publish_event(release_cmd.model_dump(mode="json"))
        await self._order_events.publish_event(cancellation.model_dump(mode="json"))
        await self._notifications.publish_event(
            {
                "event_type": "notification.order-cancelled",
                "order_id": str(order.order_id),
                "user_id": order.payload.user_id,
                "reason": failed.reason,
            }
        )
        logger.info("Order %s cancelled (payment failure: %s)", order.order_id, failed.reason)

    async def close(self) -> None:
        await asyncio.gather(
            self._inventory.stop(),
            self._payment.stop(),
            self._order_events.stop(),
            self._notifications.stop(),
        )
        logger.info("Saga runner shut down")