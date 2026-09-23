from datetime import date
from pydantic import BaseModel, Field


class InventoryRow(BaseModel):
    snapshot_date: date
    product_id: int = Field(gt=0)
    stock_on_hand: int = Field(ge=0)
    units_received: int = Field(ge=0)
    units_sold: int = Field(ge=0)
    stockout_days: int = Field(ge=0)
    days_of_supply: float = Field(ge=0)
    fill_rate: float = Field(ge=0, le=1)
    stockout_flag: int = Field(ge=0, le=1)
    overstock_flag: int = Field(ge=0, le=1)
    reorder_flag: int = Field(ge=0, le=1)
    sell_through_rate: float = Field(ge=0)
    product_name: str
    category: str
    segment: str
    year: int = Field(ge=2012)
    month: int = Field(ge=1, le=12)
