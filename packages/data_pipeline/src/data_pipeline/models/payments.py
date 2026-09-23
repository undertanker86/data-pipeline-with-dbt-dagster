from decimal import Decimal
from pydantic import BaseModel, Field


class PaymentRow(BaseModel):
    order_id: int = Field(gt=0)
    payment_method: str
    payment_value: Decimal = Field(max_digits=15, decimal_places=2, gt=0)
    installments: int = Field(ge=1)
