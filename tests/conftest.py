import pytest
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from market_risk.config import Settings
from market_risk.database.engine import Base
from market_risk.database.repository import MarketDataRepository


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
        use_local_source=True,
        local_data_path=str(tmp_path),
        redis_url=None,
    )
