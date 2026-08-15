from datetime import date, datetime

from sqlalchemy import Date, DateTime, Float, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from market_risk.database.engine import Base


class MarketPrice(Base):
    __tablename__ = "market_prices"

    id: Mapped[int] = mapped_column(primary_key=True)
    ticker: Mapped[str] = mapped_column(String(10), index=True)
    date: Mapped[date] = mapped_column(Date, index=True)
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low: Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    volume: Mapped[int] = mapped_column(Integer)

    __table_args__ = (UniqueConstraint("ticker", "date", name="uq_ticker_date"),)


class ComputedMetric(Base):
    __tablename__ = "computed_metrics"

    id: Mapped[int] = mapped_column(primary_key=True)
    ticker: Mapped[str] = mapped_column(String(10), index=True)
    computed_at: Mapped[datetime] = mapped_column(DateTime)
    window_start: Mapped[date] = mapped_column(Date)
    window_end: Mapped[date] = mapped_column(Date)
    annualized_volatility: Mapped[float] = mapped_column(Float)
    var_95: Mapped[float] = mapped_column(Float)
    var_99: Mapped[float] = mapped_column(Float)
    cvar_95: Mapped[float] = mapped_column(Float)
    cvar_99: Mapped[float] = mapped_column(Float)
    sharpe_ratio: Mapped[float] = mapped_column(Float)
    max_drawdown: Mapped[float] = mapped_column(Float)

    __table_args__ = (
        UniqueConstraint(
            "ticker", "window_start", "window_end", name="uq_metric_ticker_window"
        ),
    )


class Portfolio(Base):
    __tablename__ = "portfolios"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100), unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime)

    holdings: Mapped[list["PortfolioHolding"]] = relationship(
        back_populates="portfolio", cascade="all, delete-orphan"
    )


class PortfolioHolding(Base):
    __tablename__ = "portfolio_holdings"

    id: Mapped[int] = mapped_column(primary_key=True)
    portfolio_id: Mapped[int] = mapped_column(ForeignKey("portfolios.id"), index=True)
    ticker: Mapped[str] = mapped_column(String(10))
    weight: Mapped[float] = mapped_column(Float)

    portfolio: Mapped["Portfolio"] = relationship(back_populates="holdings")

    __table_args__ = (
        UniqueConstraint("portfolio_id", "ticker", name="uq_portfolio_ticker"),
    )
