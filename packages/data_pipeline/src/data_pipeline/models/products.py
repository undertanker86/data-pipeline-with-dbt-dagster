from decimal import Decimal, ROUND_HALF_UP
from pydantic import BaseModel, Field, field_validator


class ProductRow(BaseModel):
    product_id: int = Field(gt=0)
    product_name: str
    category: str
    segment: str
    size: str
    color: str
    price: Decimal = Field(max_digits=15, decimal_places=2, gt=0)
    cogs: Decimal = Field(max_digits=15, decimal_places=2, gt=0)

    @field_validator("price", "cogs", mode="before")
    @classmethod
    def _round_money(cls, v):
        return Decimal(str(v)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
