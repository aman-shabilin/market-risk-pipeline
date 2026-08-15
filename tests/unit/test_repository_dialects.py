"""The upsert path must not be hardwired to SQLite.

``upsert_prices`` previously always used ``sqlalchemy.dialects.sqlite.insert``,
which fails on any other backend despite ``database_url`` being configurable.
"""

from datetime import date

import pytest
from sqlalchemy import func, select

from market_risk.database.models import MarketPrice
from market_risk.database.repository import MarketDataRepository

DIALECTS = ["sqlite", "postgresql", "mysql"]


def record(day: int, ticker: str = "AAPL", close: float = 185.9) -> dict[str, object]:
    return {
        "ticker": ticker,
        "date": date(2024, 1, day),
        "open": 185.5,
        "high": 186.2,
        "low": 184.8,
        "close": close,
        "volume": 50_000_000,
    }


@pytest.fixture
def repo_with_dialect(db_session, monkeypatch):
    """Return a factory that forces the repository to report a given dialect."""

    def _factory(dialect: str) -> MarketDataRepository:
        repo = MarketDataRepository(db_session)
        monkeypatch.setattr(
            type(repo), "_dialect", property(lambda self: dialect), raising=True
        )
        return repo

    return _factory


class TestDialectAgnosticUpsert:
    @pytest.mark.parametrize("dialect", DIALECTS)
    def test_insert_succeeds(self, repo_with_dialect, dialect):
        repo = repo_with_dialect(dialect)
        assert repo.upsert_prices([record(2), record(3)]) == 2

    @pytest.mark.parametrize("dialect", DIALECTS)
    def test_reinsert_is_idempotent(self, repo_with_dialect, dialect):
        repo = repo_with_dialect(dialect)
        records = [record(2), record(3)]
        repo.upsert_prices(records)
        assert repo.upsert_prices(records) == 0

    @pytest.mark.parametrize("dialect", DIALECTS)
    def test_no_duplicate_rows_persisted(self, repo_with_dialect, db_session, dialect):
        repo = repo_with_dialect(dialect)
        records = [record(2), record(3)]
        repo.upsert_prices(records)
        repo.upsert_prices(records)

        total = db_session.scalar(select(func.count()).select_from(MarketPrice))
        assert total == 2

    @pytest.mark.parametrize("dialect", DIALECTS)
    def test_partial_overlap_inserts_only_new(self, repo_with_dialect, dialect):
        repo = repo_with_dialect(dialect)
        repo.upsert_prices([record(2), record(3)])
        assert repo.upsert_prices([record(3), record(4), record(5)]) == 2

    @pytest.mark.parametrize("dialect", DIALECTS)
    def test_empty_input(self, repo_with_dialect, dialect):
        assert repo_with_dialect(dialect).upsert_prices([]) == 0

    @pytest.mark.parametrize("dialect", DIALECTS)
    def test_same_date_different_tickers_both_inserted(self, repo_with_dialect, dialect):
        repo = repo_with_dialect(dialect)
        inserted = repo.upsert_prices(
            [record(2, ticker="AAPL"), record(2, ticker="MSFT")]
        )
        assert inserted == 2

    def test_fallback_dedupes_within_a_single_batch(self, repo_with_dialect, db_session):
        """A batch containing its own duplicates must not violate the constraint."""
        repo = repo_with_dialect("mysql")
        inserted = repo.upsert_prices([record(2), record(2), record(3)])

        assert inserted == 2
        total = db_session.scalar(select(func.count()).select_from(MarketPrice))
        assert total == 2

    def test_all_dialects_agree_on_result(self, db_session, monkeypatch):
        """SQLite, PostgreSQL and the generic path report the same counts."""
        results = []
        for dialect in DIALECTS:
            db_session.query(MarketPrice).delete()
            db_session.commit()

            repo = MarketDataRepository(db_session)
            monkeypatch.setattr(
                type(repo), "_dialect", property(lambda self, d=dialect: d), raising=True
            )
            first = repo.upsert_prices([record(2), record(3)])
            second = repo.upsert_prices([record(2), record(3)])
            results.append((first, second))

        assert len(set(results)) == 1
