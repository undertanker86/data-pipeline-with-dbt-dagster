import datetime
from decimal import Decimal
from pydantic import BaseModel, Field


class SalesRow(BaseModel):
    date: datetime.date = Field(alias="Date")
    revenue: Decimal = Field(max_digits=15, decimal_places=2, ge=0, alias="Revenue")
    cogs: Decimal = Field(max_digits=15, decimal_places=2, ge=0, alias="COGS")
