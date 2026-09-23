from decimal import Decimal
from typing import Optional
from pydantic import BaseModel, Field


class OrderItemRow(BaseModel):
    order_id: int = Field(gt=0)
    product_id: int = Field(gt=0)
    quantity: int = Field(gt=0)
    unit_price: Decimal = Field(max_digits=15, decimal_places=2, gt=0)
    discount_amount: Decimal = Field(default=Decimal("0"), max_digits=15, decimal_places=2, ge=0)
    promo_id: Optional[str] = None
    promo_id_2: Optional[str] = None
