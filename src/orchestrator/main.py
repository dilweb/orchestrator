import asyncio
import logging
import signal
from typing import Any, Awaitable, Callable

from orchestrator.kafka.consumer import OrderKafkaConsumer
import orchestrator.kafka.producer as producers
from orchestrator.saga.runner import OrderSagaRunner

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)


async def dispatch_event(event: dict[str, Any], saga: OrderSagaRunner) -> None:
    event_type = event.get("event_type")
    if event_type == "order.created":
        await saga.handle_order_created(event)
    elif event_type == "inventory.reserved":
        await saga.handle_inventory_reserved(event)
    elif event_type == "inventory.reserve-failed":
        await saga.handle_inventory_reserve_failed(event)
    elif event_type == "payment.succeeded":
        await saga.handle_payment_succeeded(event)
    elif event_type == "payment.failed":
        await saga.handle_payment_failed(event)
    else:
        logger.debug("Skip unsupported event_type=%s", event_type)


async def run() -> None:
    inventory_producer = producers.get_inventory_producer()
    payment_producer = producers.get_payment_producer()
    order_events_producer = producers.get_order_events_producer()
    notifications_producer = producers.get_notifications_producer()
    await asyncio.gather(
        inventory_producer.start(),
        payment_producer.start(),
        order_events_producer.start(),
        notifications_producer.start(),
    )

    saga = OrderSagaRunner(
        inventory_producer=inventory_producer,
        payment_producer=payment_producer,
        order_events_producer=order_events_producer,
        notifications_producer=notifications_producer,
    )

    consumer = OrderKafkaConsumer(group_id="orchestrator-orders")
    await consumer.start()

    stop_event = asyncio.Event()

    def _stop(*_: object) -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _stop)

    async def _handler(event: dict[str, Any]) -> None:
        try:
            await dispatch_event(event, saga)
        except Exception:
            logger.exception("Saga failed for order_id=%s", event.get("order_id"))

    consume_task = asyncio.create_task(consumer.consume(_handler))
    await stop_event.wait()
    consume_task.cancel()

    await consumer.stop()
    await saga.close()


if __name__ == "__main__":
    asyncio.run(run())