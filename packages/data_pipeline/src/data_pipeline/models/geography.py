from pydantic import BaseModel, Field


class GeographyRow(BaseModel):
    zip: int = Field(gt=0)
    city: str
    region: str
    district: str
