
import pytest

from market_risk.database.engine import Base, get_engine, get_session_factory
from market_risk.database.repository import MarketDataRepository
from market_risk.ingestion.local_source import LocalSource
from market_risk.pipeline.orchestrator import PipelineOrchestrator


@pytest.fixture
def pipeline_env(tmp_path):
    csv_content = (
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
    (tmp_path / "data.csv").write_text(csv_content)

    engine = get_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session_factory = get_session_factory(engine)
    session = session_factory()

    source = LocalSource(str(tmp_path))
    repo = MarketDataRepository(session)

    return source, repo, session


class TestPipelineOrchestrator:
    def test_full_run(self, pipeline_env):
        source, repo, session = pipeline_env
        orchestrator = PipelineOrchestrator(source=source, repository=repo)
        result = orchestrator.run()

        assert result.rows_ingested == 10
        assert result.validation_errors == 0
        assert set(result.tickers_processed) == {"AAPL", "MSFT"}

    def test_ticker_filter(self, pipeline_env):
        source, repo, session = pipeline_env
        orchestrator = PipelineOrchestrator(source=source, repository=repo)
        result = orchestrator.run(ticker_filter="AAPL")

        assert result.tickers_processed == ["AAPL"]

    def test_metrics_computed(self, pipeline_env):
        source, repo, session = pipeline_env
        orchestrator = PipelineOrchestrator(source=source, repository=repo)
        orchestrator.run()

        metrics = repo.get_latest_metrics("AAPL")
        assert metrics is not None
        assert metrics.ticker == "AAPL"
        assert metrics.annualized_volatility > 0

    def test_idempotent_ingestion(self, pipeline_env):
        source, repo, session = pipeline_env
        orchestrator = PipelineOrchestrator(source=source, repository=repo)
        result1 = orchestrator.run()
        result2 = orchestrator.run()

        assert result1.rows_ingested == 10
        assert result2.rows_ingested == 0

    def test_validation_errors_counted(self, tmp_path):
        csv_content = (
            "ticker,date,open,high,low,close,volume\n"
            "AAPL,2024-01-02,185.5,186.2,184.8,185.9,50000000\n"
            "AAPL,2024-01-03,185.9,180.0,185.0,186.5,48000000\n"  # high < low
            "AAPL,invalid-date,186.5,186.8,184.2,184.5,52000000\n"  # bad date
        )
        (tmp_path / "bad.csv").write_text(csv_content)

        engine = get_engine("sqlite:///:memory:")
        Base.metadata.create_all(bind=engine)
        session = get_session_factory(engine)()

        source = LocalSource(str(tmp_path))
        repo = MarketDataRepository(session)
        orchestrator = PipelineOrchestrator(source=source, repository=repo)
        result = orchestrator.run()

        assert result.validation_errors == 2
        assert result.rows_ingested == 1
