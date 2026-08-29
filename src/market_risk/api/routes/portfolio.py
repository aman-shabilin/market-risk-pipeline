from datetime import UTC, datetime

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import delete as sql_delete
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import selectinload

from market_risk.api.deps import get_repository
from market_risk.database.models import Portfolio, PortfolioHolding
from market_risk.database.repository import MarketDataRepository
from market_risk.metrics import compute_daily_returns
from market_risk.metrics.portfolio import compute_portfolio_risk
from market_risk.schemas.portfolio import (
    PortfolioCreate,
    PortfolioResponse,
    PortfolioRiskResponse,
    PortfolioUpdate,
)

router = APIRouter(prefix="/api/v1/portfolios", tags=["portfolios"])


@router.post("/", response_model=PortfolioResponse, status_code=201)
def create_portfolio(
    body: PortfolioCreate,
    repo: MarketDataRepository = Depends(get_repository),
) -> PortfolioResponse:
    session = repo.session

    total_weight = sum(h.weight for h in body.holdings)
    portfolio = Portfolio(
        name=body.name,
        created_at=datetime.now(UTC),
        holdings=[
            PortfolioHolding(
                ticker=h.ticker, weight=h.weight / total_weight
            )
            for h in body.holdings
        ],
    )
    session.add(portfolio)
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        raise HTTPException(
            status_code=409,
            detail=f"Portfolio '{body.name}' already exists",
        )
    session.refresh(portfolio)

    return PortfolioResponse(
        id=portfolio.id,
        name=portfolio.name,
        created_at=portfolio.created_at,
        holdings=[
            {"ticker": h.ticker, "weight": h.weight}
            for h in portfolio.holdings
        ],
    )


@router.get("/", response_model=list[PortfolioResponse])
def list_portfolios(
    limit: int = Query(50, ge=1, le=500, description="Max portfolios to return"),
    offset: int = Query(0, ge=0, description="Portfolios to skip"),
    repo: MarketDataRepository = Depends(get_repository),
) -> list[PortfolioResponse]:
    session = repo.session
    portfolios = list(
        session.scalars(
            select(Portfolio)
            .options(selectinload(Portfolio.holdings))
            .offset(offset)
            .limit(limit)
        ).all()
    )
    return [
        PortfolioResponse(
            id=p.id,
            name=p.name,
            created_at=p.created_at,
            holdings=[
                {"ticker": h.ticker, "weight": h.weight}
                for h in p.holdings
            ],
        )
        for p in portfolios
    ]


@router.put("/{name}", response_model=PortfolioResponse)
def update_portfolio(
    name: str,
    body: PortfolioUpdate,
    repo: MarketDataRepository = Depends(get_repository),
) -> PortfolioResponse:
    session = repo.session
    portfolio = session.scalar(
        select(Portfolio).options(selectinload(Portfolio.holdings)).where(Portfolio.name == name)
    )
    if not portfolio:
        raise HTTPException(status_code=404, detail=f"Portfolio '{name}' not found")

    session.execute(
        sql_delete(PortfolioHolding).where(PortfolioHolding.portfolio_id == portfolio.id)
    )

    total_weight = sum(h.weight for h in body.holdings)
    new_holdings = [
        PortfolioHolding(
            portfolio_id=portfolio.id,
            ticker=h.ticker,
            weight=h.weight / total_weight,
        )
        for h in body.holdings
    ]
    session.add_all(new_holdings)
    session.commit()

    session.refresh(portfolio)
    return PortfolioResponse(
        id=portfolio.id,
        name=portfolio.name,
        created_at=portfolio.created_at,
        holdings=[
            {"ticker": h.ticker, "weight": h.weight}
            for h in portfolio.holdings
        ],
    )


@router.delete("/{name}", status_code=204)
def delete_portfolio(
    name: str,
    repo: MarketDataRepository = Depends(get_repository),
) -> None:
    session = repo.session
    portfolio = session.scalar(select(Portfolio).where(Portfolio.name == name))
    if not portfolio:
        raise HTTPException(status_code=404, detail=f"Portfolio '{name}' not found")

    session.delete(portfolio)
    session.commit()


@router.get("/{name}/risk", response_model=PortfolioRiskResponse)
def get_portfolio_risk(
    name: str,
    request: Request,
    repo: MarketDataRepository = Depends(get_repository),
) -> PortfolioRiskResponse:
    session = repo.session
    portfolio = session.scalar(select(Portfolio).where(Portfolio.name == name))
    if not portfolio:
        raise HTTPException(status_code=404, detail=f"Portfolio '{name}' not found")

    weights = {h.ticker: h.weight for h in portfolio.holdings}

    returns_by_ticker: dict[str, pd.Series] = {}
    for ticker in weights:
        prices = repo.get_prices(ticker)
        if len(prices) < 3:
            continue
        close_prices = pd.Series([p.close for p in prices])
        returns_by_ticker[ticker] = compute_daily_returns(close_prices)

    if not returns_by_ticker:
        raise HTTPException(
            status_code=404,
            detail="No price data available for portfolio tickers. Run ingest first.",
        )

    rfr = request.app.state.settings.risk_free_rate
    result = compute_portfolio_risk(returns_by_ticker, weights, risk_free_rate=rfr)
    if result is None:
        raise HTTPException(status_code=404, detail="Insufficient data to compute portfolio risk")

    return PortfolioRiskResponse(
        portfolio_name=portfolio.name,
        annualized_volatility=round(result.annualized_volatility, 6),
        var_95=round(result.var_95, 6),
        var_99=round(result.var_99, 6),
        cvar_95=round(result.cvar_95, 6),
        cvar_99=round(result.cvar_99, 6),
        sharpe_ratio=round(result.sharpe_ratio, 6),
        max_drawdown=round(result.max_drawdown, 6),
        diversification_ratio=round(result.diversification_ratio, 4),
    )
