from datetime import date

from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from market_risk.database.models import MarketPrice, ComputedMetric


class MarketDataRepository:
    def __init__(self, session: Session):
        self.session = session

    def upsert_prices(self, records: list[dict]) -> int:
        """Insert or ignore price records. Returns count of new rows."""
        if not records:
            return 0
        stmt = sqlite_insert(MarketPrice).values(records)
        stmt = stmt.on_conflict_do_nothing(index_elements=["ticker", "date"])
        result = self.session.execute(stmt)
        self.session.commit()
        return result.rowcount  # type: ignore[return-value]

    def get_prices(
        self, ticker: str, start: date | None = None, end: date | None = None
    ) -> list[MarketPrice]:
        query = select(MarketPrice).where(MarketPrice.ticker == ticker)
        if start:
            query = query.where(MarketPrice.date >= start)
        if end:
            query = query.where(MarketPrice.date <= end)
        query = query.order_by(MarketPrice.date)
        return list(self.session.scalars(query).all())

    def save_metrics(self, metrics: ComputedMetric) -> None:
        self.session.add(metrics)
        self.session.commit()

    def get_latest_metrics(self, ticker: str) -> ComputedMetric | None:
        query = (
            select(ComputedMetric)
            .where(ComputedMetric.ticker == ticker)
            .order_by(ComputedMetric.computed_at.desc())
            .limit(1)
        )
        return self.session.scalar(query)

    def list_tickers(self) -> list[str]:
        query = select(MarketPrice.ticker).distinct().order_by(MarketPrice.ticker)
        return list(self.session.scalars(query).all())
