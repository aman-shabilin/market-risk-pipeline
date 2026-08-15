"""Shared risk-metric computation over stored price rows.

Both the ingestion pipeline and the API depend on this module so that a
precomputed metric row and an on-the-fly windowed computation always use
identical maths.
"""

import math
from dataclasses import dataclass
from datetime import date

import pandas as pd

from market_risk.database.models import MarketPrice
from market_risk.metrics.cvar import conditional_var
from market_risk.metrics.drawdown import max_drawdown
from market_risk.metrics.returns import compute_daily_returns
from market_risk.metrics.sharpe import sharpe_ratio
from market_risk.metrics.var import historical_var
from market_risk.metrics.volatility import annualized_volatility

MIN_PRICE_POINTS = 3
"""Minimum price observations needed for a meaningful metric set."""


@dataclass(frozen=True)
class RiskMetrics:
    """Risk metrics for a single ticker over a closed date window."""

    ticker: str
    window_start: date
    window_end: date
    annualized_volatility: float
    var_95: float
    var_99: float
    cvar_95: float
    cvar_99: float
    sharpe_ratio: float
    max_drawdown: float


def compute_risk_metrics(ticker: str, price_rows: list[MarketPrice]) -> RiskMetrics | None:
    """Compute the full metric set for ``price_rows``.

    Returns ``None`` when there is too little data or any metric is not
    finite, so callers never persist or serve NaN values.
    """
    if len(price_rows) < MIN_PRICE_POINTS:
        return None

    prices = pd.Series([row.close for row in price_rows], dtype="float64")
    dates = [row.date for row in price_rows]
    returns = compute_daily_returns(prices)

    values = {
        "annualized_volatility": annualized_volatility(returns),
        "var_95": historical_var(returns, confidence=0.95),
        "var_99": historical_var(returns, confidence=0.99),
        "cvar_95": conditional_var(returns, confidence=0.95),
        "cvar_99": conditional_var(returns, confidence=0.99),
        "sharpe_ratio": sharpe_ratio(returns),
        "max_drawdown": max_drawdown(prices),
    }

    if any(not math.isfinite(value) for value in values.values()):
        return None

    return RiskMetrics(
        ticker=ticker,
        window_start=dates[0],
        window_end=dates[-1],
        **values,
    )
