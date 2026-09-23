from enum import Enum
from datetime import date
from pydantic import BaseModel, Field


class OrderStatus(str, Enum):
    cancelled = "cancelled"
    created = "created"
    delivered = "delivered"
    paid = "paid"
    returned = "returned"
    shipped = "shipped"


class PaymentMethod(str, Enum):
    apple_pay = "apple_pay"
    credit_card = "credit_card"
    cod = "cod"
    bank_transfer = "bank_transfer"
    e_wallet = "e_wallet"
    paypal = "paypal"


class DeviceType(str, Enum):
    desktop = "desktop"
    mobile = "mobile"
    tablet = "tablet"


class OrderSource(str, Enum):
    paid_search = "paid_search"
    organic_search = "organic_search"
    social_media = "social_media"
    email_campaign = "email_campaign"
    direct = "direct"
    referral = "referral"


class OrderRow(BaseModel):
    order_id: int = Field(gt=0)
    order_date: date
    customer_id: int = Field(gt=0)
    zip: int = Field(gt=0)
    order_status: OrderStatus
    payment_method: PaymentMethod
    device_type: DeviceType
    order_source: OrderSource
