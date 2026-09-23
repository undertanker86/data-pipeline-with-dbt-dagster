from decimal import Decimal
from datetime import date
from pydantic import BaseModel, Field, model_validator


class ShipmentRow(BaseModel):
    order_id: int = Field(gt=0)
    ship_date: date
    delivery_date: date
    shipping_fee: Decimal = Field(max_digits=15, decimal_places=2, ge=0)

    @model_validator(mode="after")
    def check_dates(self):
        if self.ship_date > self.delivery_date:
            raise ValueError(f"ship_date {self.ship_date} after delivery_date {self.delivery_date}")
        return self
