from market_risk.database.engine import Base, get_engine, get_session_factory
from market_risk.database.repository import MarketDataRepository
from market_risk.ingestion.local_source import LocalSource
from market_risk.pipeline.orchestrator import PipelineOrchestrator


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

    def test_finished_at_is_populated(self, pipeline_env):
        source, repo, _ = pipeline_env
        orchestrator = PipelineOrchestrator(source=source, repository=repo)
        result = orchestrator.run()

        assert result.finished_at is not None

    def test_finished_at_populated_when_no_files(self, tmp_path):
        engine = get_engine("sqlite:///:memory:")
        Base.metadata.create_all(bind=engine)
        session = get_session_factory(engine)()

        orchestrator = PipelineOrchestrator(
            source=LocalSource(str(tmp_path)),
            repository=MarketDataRepository(session),
        )
        result = orchestrator.run()

        assert result.rows_ingested == 0
        assert result.tickers_processed == []
        assert result.finished_at is not None
