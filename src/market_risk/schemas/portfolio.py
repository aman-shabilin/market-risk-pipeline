from datetime import datetime

import pydantic
from pydantic import BaseModel, field_validator


class HoldingInput(BaseModel):
    ticker: str
    weight: float

    @field_validator("ticker")
    @classmethod
    def ticker_uppercase(cls, v: str) -> str:
        return v.upper().strip()

    @field_validator("weight")
    @classmethod
    def weight_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("weight must be positive")
        return v


class PortfolioCreate(BaseModel):
    name: str = pydantic.Field(max_length=100)
    holdings: list[HoldingInput]

    @field_validator("holdings")
    @classmethod
    def at_least_one_holding(cls, v: list[HoldingInput]) -> list[HoldingInput]:
        if len(v) < 1:
            raise ValueError("portfolio must have at least one holding")
        return v


class PortfolioUpdate(BaseModel):
    holdings: list[HoldingInput]

    @field_validator("holdings")
    @classmethod
    def at_least_one_holding(cls, v: list[HoldingInput]) -> list[HoldingInput]:
        if len(v) < 1:
            raise ValueError("portfolio must have at least one holding")
        return v


class HoldingResponse(BaseModel):
    ticker: str
    weight: float


class PortfolioResponse(BaseModel):
    id: int
    name: str
    created_at: datetime
    holdings: list[HoldingResponse]


class PortfolioRiskResponse(BaseModel):
    portfolio_name: str
    annualized_volatility: float
    var_95: float
    var_99: float
    cvar_95: float
    cvar_99: float
    sharpe_ratio: float
    max_drawdown: float
    diversification_ratio: float
