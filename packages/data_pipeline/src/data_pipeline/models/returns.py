from decimal import Decimal
from datetime import date
from pydantic import BaseModel, Field


class ReturnRow(BaseModel):
    return_id: str = Field(min_length=1)
    order_id: int = Field(gt=0)
    product_id: int = Field(gt=0)
    return_date: date
    return_reason: str
    return_quantity: int = Field(gt=0)
    refund_amount: Decimal = Field(max_digits=15, decimal_places=2, ge=0)
