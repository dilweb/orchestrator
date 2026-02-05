from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field

from orchestrator.models.order import OrderItem


class MessageBase(BaseModel):
  order_id: UUID
  saga_id: UUID
  message_id: UUID
  correlation_id: UUID


class InventoryReserveCommand(MessageBase):
  event_type: Literal["inventory.reserve"] = "inventory.reserve"
  items: list[OrderItem]


class InventoryReserved(MessageBase):
  event_type: Literal["inventory.reserved"] = "inventory.reserved"
  reserved_at: datetime = Field(default_factory=datetime.now)


class InventoryReserveFailed(MessageBase):
  event_type: Literal["inventory.reserve-failed"] = "inventory.reserve-failed"
  reason: str


class InventoryReservationCancelled(MessageBase):
  event_type: Literal["inventory.reservation-cancelled"] = "inventory.reservation-cancelled"
  reason: str | None = None


class PaymentChargeCommand(MessageBase):
  event_type: Literal["payment.charge"] = "payment.charge"
  amount: str  # Decimal → сериализуем как строку
  currency: Literal["USD", "KZT", "UAH", "RUB"]


class PaymentSucceeded(MessageBase):
  event_type: Literal["payment.succeeded"] = "payment.succeeded"
  processed_at: datetime = Field(default_factory=datetime.now)
  transaction_id: str


class PaymentFailed(MessageBase):
  event_type: Literal["payment.failed"] = "payment.failed"
  reason: str
  retryable: bool = True


class PaymentRefunded(MessageBase):
  event_type: Literal["payment.refunded"] = "payment.refunded"
  refund_id: str
  refunded_at: datetime = Field(default_factory=datetime.now)


class OrderCompleted(MessageBase):
  event_type: Literal["order.completed"] = "order.completed"
  completed_at: datetime = Field(default_factory=datetime.now)


class OrderCancelled(MessageBase):
  event_type: Literal["order.cancelled"] = "order.cancelled"
  reason: str