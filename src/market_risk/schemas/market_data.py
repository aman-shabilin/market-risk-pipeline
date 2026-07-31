from datetime import date

from pydantic import BaseModel, field_validator, model_validator


class MarketDataRow(BaseModel):
    ticker: str
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int

    @field_validator("ticker")
    @classmethod
    def ticker_uppercase(cls, v: str) -> str:
        return v.upper().strip()

    @field_validator("volume")
    @classmethod
    def volume_non_negative(cls, v: int) -> int:
        if v < 0:
            raise ValueError("volume must be non-negative")
        return v

    @model_validator(mode="after")
    def high_gte_low(self) -> "MarketDataRow":
        if self.high < self.low:
            raise ValueError("high must be >= low")
        return self
