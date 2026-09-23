from datetime import date
from pydantic import BaseModel, Field


class WebTrafficRow(BaseModel):
    date: date
    sessions: int = Field(ge=0)
    unique_visitors: int = Field(ge=0)
    page_views: int = Field(ge=0)
    bounce_rate: float = Field(ge=0, le=1)
    avg_session_duration_sec: float = Field(ge=0)
    traffic_source: str
