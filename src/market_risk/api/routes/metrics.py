from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from market_risk.api.cache import CacheBackend
from market_risk.api.deps import get_cache, get_repository
from market_risk.database.repository import MarketDataRepository
from market_risk.metrics import MIN_PRICE_POINTS, compute_daily_returns, compute_risk_metrics
from market_risk.metrics.var import historical_var
from market_risk.metrics.volatility import rolling_volatility
from market_risk.schemas.metrics import ReturnsResponse, RiskMetricsResponse, RollingMetricsPoint

router = APIRouter(prefix="/api/v1/metrics", tags=["metrics"])

PRECISION = 6


@router.get("/tickers", response_model=list[str])
def list_tickers(
    limit: int = Query(100, ge=1, le=1000, description="Max tickers to return"),
    offset: int = Query(0, ge=0, description="Tickers to skip"),
    repo: MarketDataRepository = Depends(get_repository),
) -> list[str]:
    tickers = repo.list_tickers()
    return tickers[offset : offset + limit]


@router.get("/{ticker}", response_model=RiskMetricsResponse)
async def get_metrics(
    request: Request,
    ticker: str,
    start_date: date | None = Query(
        None, description="Inclusive start of the window. Metrics are computed on demand."
    ),
    end_date: date | None = Query(
        None, description="Inclusive end of the window. Metrics are computed on demand."
    ),
    repo: MarketDataRepository = Depends(get_repository),
    cache: CacheBackend = Depends(get_cache),
) -> RiskMetricsResponse:
    ticker = ticker.upper()

    if start_date and end_date and start_date > end_date:
        raise HTTPException(status_code=422, detail="start_date must not be after end_date")

    ttl: int = request.app.state.settings.cache_ttl_seconds
    cache_key = f"metrics:{ticker}:{start_date}:{end_date}"

    cached = await cache.get(cache_key)
    if cached:
        return RiskMetricsResponse.model_validate_json(cached)

    if start_date or end_date:
        response = _windowed_metrics(repo, ticker, start_date, end_date)
    else:
        response = _latest_metrics(repo, ticker)

    await cache.set(cache_key, response.model_dump_json(), ttl=ttl)
    return response


def _latest_metrics(repo: MarketDataRepository, ticker: str) -> RiskMetricsResponse:
    """Serve the most recent metric row persisted by the ingestion pipeline."""
    metric = repo.get_latest_metrics(ticker)
    if not metric:
        raise HTTPException(status_code=404, detail=f"No metrics found for {ticker}")

    return RiskMetricsResponse(
        ticker=metric.ticker,
        start_date=metric.window_start,
        end_date=metric.window_end,
        annualized_volatility=round(metric.annualized_volatility, PRECISION),
        var_95=round(metric.var_95, PRECISION),
        var_99=round(metric.var_99, PRECISION),
        cvar_95=round(metric.cvar_95, PRECISION),
        cvar_99=round(metric.cvar_99, PRECISION),
        sharpe_ratio=round(metric.sharpe_ratio, PRECISION),
        max_drawdown=round(metric.max_drawdown, PRECISION),
    )


def _windowed_metrics(
    repo: MarketDataRepository,
    ticker: str,
    start_date: date | None,
    end_date: date | None,
) -> RiskMetricsResponse:
    """Compute metrics on the fly for an explicit date window."""
    price_rows = repo.get_prices(ticker, start=start_date, end=end_date)
    if not price_rows:
        raise HTTPException(
            status_code=404,
            detail=f"No price data for {ticker} in the requested window",
        )

    metrics = compute_risk_metrics(ticker, price_rows)
    if metrics is None:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Window contains {len(price_rows)} price points; "
                f"at least {MIN_PRICE_POINTS} are required"
            ),
        )

    return RiskMetricsResponse(
        ticker=metrics.ticker,
        start_date=metrics.window_start,
        end_date=metrics.window_end,
        annualized_volatility=round(metrics.annualized_volatility, PRECISION),
        var_95=round(metrics.var_95, PRECISION),
        var_99=round(metrics.var_99, PRECISION),
        cvar_95=round(metrics.cvar_95, PRECISION),
        cvar_99=round(metrics.cvar_99, PRECISION),
        sharpe_ratio=round(metrics.sharpe_ratio, PRECISION),
        max_drawdown=round(metrics.max_drawdown, PRECISION),
    )


@router.get("/{ticker}/returns", response_model=ReturnsResponse)
def get_returns(
    ticker: str,
    start_date: date | None = Query(None, description="Inclusive start date"),
    end_date: date | None = Query(None, description="Inclusive end date"),
    repo: MarketDataRepository = Depends(get_repository),
) -> ReturnsResponse:
    """Return daily return series for distribution visualization."""
    ticker = ticker.upper()
    price_rows = repo.get_prices(ticker, start=start_date, end=end_date)

    if len(price_rows) < MIN_PRICE_POINTS:
        raise HTTPException(
            status_code=404,
            detail=f"Insufficient price data for {ticker} to compute returns",
        )

    import pandas as pd

    prices = pd.Series([r.close for r in price_rows], dtype="float64")
    returns = compute_daily_returns(prices)
    dates = [price_rows[i].date for i in range(1, len(price_rows))]

    return ReturnsResponse(
        ticker=ticker,
        count=len(returns),
        mean=round(float(returns.mean()), PRECISION),
        std=round(float(returns.std()), PRECISION),
        min=round(float(returns.min()), PRECISION),
        max=round(float(returns.max()), PRECISION),
        dates=[d.isoformat() for d in dates],
        values=[round(float(r), PRECISION) for r in returns],
    )


@router.get("/{ticker}/rolling", response_model=list[RollingMetricsPoint])
def get_rolling_metrics(
    ticker: str,
    window: int = Query(21, ge=5, le=252, description="Rolling window in trading days"),
    start_date: date | None = Query(None, description="Inclusive start date"),
    end_date: date | None = Query(None, description="Inclusive end date"),
    repo: MarketDataRepository = Depends(get_repository),
) -> list[RollingMetricsPoint]:
    """Return rolling volatility and VaR over time for trend visualization."""
    ticker = ticker.upper()
    price_rows = repo.get_prices(ticker, start=start_date, end=end_date)

    if len(price_rows) < window + 1:
        raise HTTPException(
            status_code=422,
            detail=f"Need at least {window + 1} price points for a {window}-day rolling window",
        )

    import numpy as np
    import pandas as pd

    prices = pd.Series([r.close for r in price_rows], dtype="float64")
    returns = compute_daily_returns(prices)
    dates = [price_rows[i].date for i in range(1, len(price_rows))]

    roll_vol = rolling_volatility(returns, window=window)

    roll_var_95: list[float | None] = []
    for i in range(len(returns)):
        if i < window - 1:
            roll_var_95.append(None)
        else:
            window_returns = returns.iloc[i - window + 1 : i + 1]
            roll_var_95.append(round(historical_var(window_returns, 0.95), PRECISION))

    result: list[RollingMetricsPoint] = []
    for i in range(len(dates)):
        vol = roll_vol.iloc[i]
        result.append(RollingMetricsPoint(
            date=dates[i],
            annualized_volatility=round(float(vol), PRECISION) if not np.isnan(vol) else None,
            var_95=roll_var_95[i],
        ))

    return result
