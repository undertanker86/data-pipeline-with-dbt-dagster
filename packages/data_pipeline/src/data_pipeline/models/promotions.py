from decimal import Decimal
from datetime import date
from typing import Optional
from pydantic import BaseModel, Field, model_validator


class PromotionRow(BaseModel):
    promo_id: str = Field(min_length=1)
    promo_name: str
    promo_type: str
    discount_value: Decimal = Field(max_digits=5, decimal_places=2, ge=0)
    start_date: date
    end_date: date
    applicable_category: Optional[str] = None
    promo_channel: str
    stackable_flag: int = Field(ge=0, le=1)
    min_order_value: Decimal = Field(max_digits=15, decimal_places=2, ge=0)

    @model_validator(mode="after")
    def check_dates(self):
        if self.start_date > self.end_date:
            raise ValueError(f"start_date {self.start_date} after end_date {self.end_date}")
        return self
