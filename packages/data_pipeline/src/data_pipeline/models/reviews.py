from datetime import date
from pydantic import BaseModel, Field


class ReviewRow(BaseModel):
    review_id: str = Field(min_length=1)
    order_id: int = Field(gt=0)
    product_id: int = Field(gt=0)
    customer_id: int = Field(gt=0)
    review_date: date
    rating: int = Field(ge=1, le=5)
    review_title: str
