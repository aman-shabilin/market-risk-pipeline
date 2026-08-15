from datetime import date
from typing import Any, cast

from sqlalchemy import CursorResult, insert, select, tuple_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from market_risk.database.models import ComputedMetric, MarketPrice

_PRICE_CONFLICT_KEYS = ["ticker", "date"]

_METRIC_FIELDS = (
    "annualized_volatility",
    "var_95",
    "var_99",
    "cvar_95",
    "cvar_99",
    "sharpe_ratio",
    "max_drawdown",
)


class MarketDataRepository:
    def __init__(self, session: Session):
        self.session = session

    @property
    def _dialect(self) -> str:
        return self.session.get_bind().dialect.name

    def upsert_prices(self, records: list[dict[str, object]]) -> int:
        """Insert price records, ignoring rows that already exist.

        Uses native ``ON CONFLICT DO NOTHING`` on SQLite and PostgreSQL and
        falls back to a pre-filtered plain insert on other dialects, so the
        configured ``database_url`` is not restricted to SQLite.

        Returns the count of newly inserted rows.
        """
        if not records:
            return 0

        dialect = self._dialect

        if dialect == "sqlite":
            stmt: Any = sqlite_insert(MarketPrice).values(records)
            stmt = stmt.on_conflict_do_nothing(index_elements=_PRICE_CONFLICT_KEYS)
        elif dialect == "postgresql":
            stmt = pg_insert(MarketPrice).values(records)
            stmt = stmt.on_conflict_do_nothing(index_elements=_PRICE_CONFLICT_KEYS)
        else:
            new_records = self._filter_existing_prices(records)
            if not new_records:
                return 0
            stmt = insert(MarketPrice).values(new_records)
            self.session.execute(stmt)
            self.session.commit()
            return len(new_records)

        result = cast(CursorResult[Any], self.session.execute(stmt))
        self.session.commit()
        return result.rowcount or 0

    def _filter_existing_prices(
        self, records: list[dict[str, object]]
    ) -> list[dict[str, object]]:
        """Drop records whose (ticker, date) pair is already persisted."""
        keys = {(r["ticker"], r["date"]) for r in records}
        existing = set(
            self.session.execute(
                select(MarketPrice.ticker, MarketPrice.date).where(
                    tuple_(MarketPrice.ticker, MarketPrice.date).in_(keys)
                )
            ).all()
        )

        deduped: list[dict[str, object]] = []
        seen: set[tuple[object, object]] = set()
        for record in records:
            key = (record["ticker"], record["date"])
            if key in existing or key in seen:
                continue
            seen.add(key)
            deduped.append(record)
        return deduped

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
        """Persist metrics, replacing any existing row for the same window.

        Keeps ``computed_metrics`` bounded at one row per
        (ticker, window_start, window_end) instead of appending on every run.
        """
        existing = self.session.scalar(
            select(ComputedMetric).where(
                ComputedMetric.ticker == metrics.ticker,
                ComputedMetric.window_start == metrics.window_start,
                ComputedMetric.window_end == metrics.window_end,
            )
        )

        if existing is None:
            self.session.add(metrics)
        else:
            existing.computed_at = metrics.computed_at
            for field in _METRIC_FIELDS:
                setattr(existing, field, getattr(metrics, field))

        self.session.commit()

    def get_latest_metrics(self, ticker: str) -> ComputedMetric | None:
        query = (
            select(ComputedMetric)
            .where(ComputedMetric.ticker == ticker)
            .order_by(ComputedMetric.computed_at.desc(), ComputedMetric.id.desc())
            .limit(1)
        )
        return self.session.scalar(query)

    def list_tickers(self) -> list[str]:
        query = select(MarketPrice.ticker).distinct().order_by(MarketPrice.ticker)
        return list(self.session.scalars(query).all())
