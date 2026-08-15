from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from market_risk.api.cache import CacheBackend
from market_risk.api.deps import get_cache, get_repository
from market_risk.database.repository import MarketDataRepository
from market_risk.metrics import MIN_PRICE_POINTS, compute_risk_metrics
from market_risk.schemas.metrics import RiskMetricsResponse

router = APIRouter(prefix="/api/v1/metrics", tags=["metrics"])

PRECISION = 6


@router.get("/tickers", response_model=list[str])
def list_tickers(
    repo: MarketDataRepository = Depends(get_repository),
) -> list[str]:
    return repo.list_tickers()


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
