from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query

from market_risk.api.cache import CacheBackend
from market_risk.api.deps import get_cache, get_repository
from market_risk.database.repository import MarketDataRepository
from market_risk.schemas.metrics import RiskMetricsResponse

router = APIRouter(prefix="/api/v1/metrics", tags=["metrics"])


@router.get("/tickers", response_model=list[str])
def list_tickers(
    repo: MarketDataRepository = Depends(get_repository),
) -> list[str]:
    return repo.list_tickers()


@router.get("/{ticker}", response_model=RiskMetricsResponse)
async def get_metrics(
    ticker: str,
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    repo: MarketDataRepository = Depends(get_repository),
    cache: CacheBackend = Depends(get_cache),
) -> RiskMetricsResponse:
    ticker = ticker.upper()
    cache_key = f"metrics:{ticker}:{start_date}:{end_date}"

    cached = await cache.get(cache_key)
    if cached:
        return RiskMetricsResponse.model_validate_json(cached)

    metric = repo.get_latest_metrics(ticker)
    if not metric:
        raise HTTPException(status_code=404, detail=f"No metrics found for {ticker}")

    response = RiskMetricsResponse(
        ticker=metric.ticker,
        start_date=metric.window_start,
        end_date=metric.window_end,
        annualized_volatility=round(metric.annualized_volatility, 6),
        var_95=round(metric.var_95, 6),
        var_99=round(metric.var_99, 6),
        cvar_95=round(metric.cvar_95, 6),
        cvar_99=round(metric.cvar_99, 6),
        sharpe_ratio=round(metric.sharpe_ratio, 6),
        max_drawdown=round(metric.max_drawdown, 6),
    )

    await cache.set(cache_key, response.model_dump_json(), ttl=300)
    return response
