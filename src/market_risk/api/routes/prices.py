from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query

from market_risk.api.deps import get_repository
from market_risk.database.repository import MarketDataRepository
from market_risk.schemas.market_data import PriceHistoryResponse, PricePoint

router = APIRouter(prefix="/api/v1/prices", tags=["prices"])


@router.get("/{ticker}", response_model=PriceHistoryResponse)
def get_price_history(
    ticker: str,
    start_date: date | None = Query(None, description="Inclusive start date"),
    end_date: date | None = Query(None, description="Inclusive end date"),
    limit: int = Query(500, ge=1, le=5000, description="Max rows to return"),
    offset: int = Query(0, ge=0, description="Rows to skip"),
    repo: MarketDataRepository = Depends(get_repository),
) -> PriceHistoryResponse:
    ticker = ticker.upper()

    if start_date and end_date and start_date > end_date:
        raise HTTPException(status_code=422, detail="start_date must not be after end_date")

    prices = repo.get_prices(ticker, start=start_date, end=end_date)

    if not prices:
        raise HTTPException(status_code=404, detail=f"No price data for {ticker}")

    total = len(prices)
    page = prices[offset : offset + limit]

    return PriceHistoryResponse(
        ticker=ticker,
        total=total,
        offset=offset,
        limit=limit,
        data=[
            PricePoint(
                date=p.date,
                open=p.open,
                high=p.high,
                low=p.low,
                close=p.close,
                volume=p.volume,
            )
            for p in page
        ],
    )
