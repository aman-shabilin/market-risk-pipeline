from datetime import date, datetime

from pydantic import BaseModel


class RiskMetricsResponse(BaseModel):
    ticker: str
    start_date: date
    end_date: date
    annualized_volatility: float
    var_95: float
    var_99: float
    cvar_95: float
    cvar_99: float
    sharpe_ratio: float
    max_drawdown: float


class PipelineStatus(BaseModel):
    last_run: datetime | None
    rows_ingested: int
    errors: int
    tickers_processed: list[str]


class ReturnsResponse(BaseModel):
    ticker: str
    count: int
    mean: float
    std: float
    min: float
    max: float
    dates: list[str]
    values: list[float]


class RollingMetricsPoint(BaseModel):
    date: date
    annualized_volatility: float | None
    var_95: float | None
