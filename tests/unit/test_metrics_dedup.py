from datetime import UTC, date, datetime, timedelta

from sqlalchemy import func, select

from market_risk.database.models import ComputedMetric


def make_metric(
    ticker: str = "AAPL",
    window_start: date = date(2024, 1, 2),
    window_end: date = date(2024, 1, 12),
    volatility: float = 0.25,
    computed_at: datetime | None = None,
) -> ComputedMetric:
    return ComputedMetric(
        ticker=ticker,
        computed_at=computed_at or datetime.now(UTC),
        window_start=window_start,
        window_end=window_end,
        annualized_volatility=volatility,
        var_95=0.02,
        var_99=0.03,
        cvar_95=0.025,
        cvar_99=0.035,
        sharpe_ratio=1.5,
        max_drawdown=0.1,
    )


def metric_count(session) -> int:
    return session.scalar(select(func.count()).select_from(ComputedMetric))


class TestMetricsDedup:
    def test_repeated_save_does_not_grow_table(self, repository, db_session):
        for _ in range(5):
            repository.save_metrics(make_metric())
        assert metric_count(db_session) == 1

    def test_repeated_save_updates_values(self, repository, db_session):
        repository.save_metrics(make_metric(volatility=0.25))
        repository.save_metrics(make_metric(volatility=0.42))

        stored = repository.get_latest_metrics("AAPL")
        assert stored is not None
        assert stored.annualized_volatility == 0.42
        assert metric_count(db_session) == 1

    def test_repeated_save_refreshes_computed_at(self, repository):
        earlier = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        later = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)

        repository.save_metrics(make_metric(computed_at=earlier))
        repository.save_metrics(make_metric(computed_at=later))

        stored = repository.get_latest_metrics("AAPL")
        assert stored is not None
        assert stored.computed_at.replace(tzinfo=UTC) == later

    def test_distinct_windows_kept_separately(self, repository, db_session):
        repository.save_metrics(make_metric(window_end=date(2024, 1, 12)))
        repository.save_metrics(make_metric(window_end=date(2024, 2, 12)))
        assert metric_count(db_session) == 2

    def test_distinct_tickers_kept_separately(self, repository, db_session):
        repository.save_metrics(make_metric(ticker="AAPL"))
        repository.save_metrics(make_metric(ticker="MSFT"))
        assert metric_count(db_session) == 2

    def test_get_latest_returns_most_recent_window(self, repository):
        base = date(2024, 1, 2)
        repository.save_metrics(
            make_metric(
                window_end=base + timedelta(days=10),
                volatility=0.1,
                computed_at=datetime(2024, 1, 15, tzinfo=UTC),
            )
        )
        repository.save_metrics(
            make_metric(
                window_end=base + timedelta(days=40),
                volatility=0.9,
                computed_at=datetime(2024, 2, 15, tzinfo=UTC),
            )
        )

        stored = repository.get_latest_metrics("AAPL")
        assert stored is not None
        assert stored.annualized_volatility == 0.9

    def test_get_latest_returns_none_for_unknown_ticker(self, repository):
        assert repository.get_latest_metrics("NOPE") is None


class TestPipelineRerunIsBounded:
    def test_repeated_pipeline_runs_keep_one_row_per_ticker(self, pipeline_env):
        from market_risk.pipeline.orchestrator import PipelineOrchestrator

        source, repo, session = pipeline_env
        orchestrator = PipelineOrchestrator(source=source, repository=repo)

        for _ in range(3):
            orchestrator.run()

        # Two tickers in the fixture data, one metric row each.
        assert metric_count(session) == 2
