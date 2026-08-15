import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from market_risk.config import Settings
from market_risk.database.engine import Base, get_engine, get_session_factory
from market_risk.database.repository import MarketDataRepository
from market_risk.ingestion.local_source import LocalSource

TWO_TICKER_CSV = (
    "ticker,date,open,high,low,close,volume\n"
    "AAPL,2024-01-02,185.5,186.2,184.8,185.9,50000000\n"
    "AAPL,2024-01-03,185.9,187.1,185.0,186.5,48000000\n"
    "AAPL,2024-01-04,186.5,186.8,184.2,184.5,52000000\n"
    "AAPL,2024-01-05,184.5,185.9,183.8,185.2,47000000\n"
    "AAPL,2024-01-08,185.2,186.5,184.9,186.0,45000000\n"
    "MSFT,2024-01-02,372.5,374.8,371.2,373.9,28000000\n"
    "MSFT,2024-01-03,373.9,375.5,372.8,374.5,26000000\n"
    "MSFT,2024-01-04,374.5,375.0,371.5,372.0,30000000\n"
    "MSFT,2024-01-05,372.0,373.8,370.5,373.2,27000000\n"
    "MSFT,2024-01-08,373.2,375.0,372.5,374.8,25000000\n"
)


@pytest.fixture
def pipeline_env(tmp_path):
    """A LocalSource, repository and session wired to an in-memory database."""
    (tmp_path / "data.csv").write_text(TWO_TICKER_CSV)

    engine = get_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session = get_session_factory(engine)()

    yield LocalSource(str(tmp_path)), MarketDataRepository(session), session

    session.close()
    engine.dispose()


@pytest.fixture
def db_engine():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    yield engine
    engine.dispose()


@pytest.fixture
def db_session(db_engine):
    session_factory = sessionmaker(bind=db_engine, autoflush=False, autocommit=False)
    session = session_factory()
    yield session
    session.rollback()
    session.close()


@pytest.fixture
def repository(db_session):
    return MarketDataRepository(db_session)


@pytest.fixture
def sample_prices():
    return pd.Series([
        100.0, 101.0, 99.5, 102.0, 101.5,
        103.0, 102.5, 104.0, 103.5, 105.0,
        104.5, 106.0, 105.5, 107.0, 106.5,
        108.0, 107.5, 109.0, 108.5, 110.0,
    ])


@pytest.fixture
def sample_returns(sample_prices):
    from market_risk.metrics import compute_daily_returns
    return compute_daily_returns(sample_prices)


@pytest.fixture
def test_settings(tmp_path):
    return Settings(
        database_url="sqlite:///:memory:",
        data_source="local",
        local_data_path=str(tmp_path),
        redis_url=None,
    )
