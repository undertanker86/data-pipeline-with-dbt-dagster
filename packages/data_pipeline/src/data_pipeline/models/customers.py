from enum import Enum
from decimal import Decimal
from datetime import date
from typing import Optional
from pydantic import BaseModel, Field, field_validator


class Gender(str, Enum):
    female = "Female"
    male = "Male"
    non_binary = "Non-binary"


class AgeGroup(str, Enum):
    _18_24 = "18-24"
    _25_34 = "25-34"
    _35_44 = "35-44"
    _45_54 = "45-54"
    _55_plus = "55+"


class AcquisitionChannel(str, Enum):
    social_media = "social_media"
    email_campaign = "email_campaign"
    organic_search = "organic_search"
    paid_search = "paid_search"
    referral = "referral"
    direct = "direct"


class CustomerRow(BaseModel):
    customer_id: int = Field(gt=0)
    zip: int = Field(gt=0)
    city: str
    signup_date: date
    gender: Gender
    age_group: AgeGroup
    acquisition_channel: AcquisitionChannel
