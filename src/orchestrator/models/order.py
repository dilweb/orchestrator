from decimal import Decimal
from datetime import datetime, timezone
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field


class OrderItem(BaseModel):
    product_id: int
    name: str
    price: Decimal = Field(..., gt=0)
    quantity: int = Field(..., gt=0)


class OrderCreate(BaseModel):
    user_id: int
    items: list[OrderItem] = Field(..., min_length=1)
    currency: Literal["USD", "KZT", "UAH", "RUB"]
    amount: Decimal = Field(..., gt=0)

    def calculate_total(self) -> Decimal:
        total = sum((item.price * item.quantity for item in self.items), Decimal("0"))
        return total


class OrderPayload(OrderCreate):
    created_at: datetime
    total: Decimal = Field(..., gt=0)


class OrderEvent(BaseModel):
    event_type: Literal["order.created"] = "order.created"
    order_id: UUID
    saga_id: UUID
    message_id: UUID
    payload: OrderPayload


class OrderAccepted(BaseModel):
    order_id: UUID
    saga_id: UUID
    message_id: UUID
    status: Literal["accepted"] = "accepted"

