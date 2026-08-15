from dataclasses import dataclass, field
from datetime import UTC, datetime

import pandas as pd
from pydantic import ValidationError

from market_risk.database.models import ComputedMetric
from market_risk.database.repository import MarketDataRepository
from market_risk.ingestion.base import DataSource
from market_risk.metrics import compute_risk_metrics
from market_risk.schemas.market_data import MarketDataRow


@dataclass
class PipelineResult:
    rows_ingested: int = 0
    validation_errors: int = 0
    tickers_processed: list[str] = field(default_factory=list)
    error_details: list[str] = field(default_factory=list)
    finished_at: datetime | None = None


class PipelineOrchestrator:
    def __init__(self, source: DataSource, repository: MarketDataRepository):
        self.source = source
        self.repository = repository

    def run(self, ticker_filter: str | None = None) -> PipelineResult:
        result = PipelineResult()

        files = self.source.list_files()
        if not files:
            result.finished_at = datetime.now(UTC)
            return result

        all_records: list[dict[str, object]] = []
        for file_path in files:
            df = self.source.read_file(file_path)
            records, errors = self._validate_dataframe(df)
            all_records.extend(records)
            result.validation_errors += errors

        if ticker_filter:
            all_records = [r for r in all_records if r["ticker"] == ticker_filter.upper()]

        result.rows_ingested = self.repository.upsert_prices(all_records)

        tickers = self.repository.list_tickers()
        if ticker_filter:
            tickers = [t for t in tickers if t == ticker_filter.upper()]

        for ticker in tickers:
            self._compute_and_store_metrics(ticker)
            result.tickers_processed.append(ticker)

        result.finished_at = datetime.now(UTC)
        return result

    def _validate_dataframe(self, df: pd.DataFrame) -> tuple[list[dict[str, object]], int]:
        records: list[dict[str, object]] = []
        errors = 0
        for _, row in df.iterrows():
            try:
                validated = MarketDataRow.model_validate(row.to_dict())
                records.append(validated.model_dump())
            except ValidationError:
                errors += 1
        return records, errors

    def _compute_and_store_metrics(self, ticker: str) -> None:
        price_rows = self.repository.get_prices(ticker)
        metrics = compute_risk_metrics(ticker, price_rows)
        if metrics is None:
            return

        self.repository.save_metrics(
            ComputedMetric(
                ticker=metrics.ticker,
                computed_at=datetime.now(UTC),
                window_start=metrics.window_start,
                window_end=metrics.window_end,
                annualized_volatility=metrics.annualized_volatility,
                var_95=metrics.var_95,
                var_99=metrics.var_99,
                cvar_95=metrics.cvar_95,
                cvar_99=metrics.cvar_99,
                sharpe_ratio=metrics.sharpe_ratio,
                max_drawdown=metrics.max_drawdown,
            )
        )


def main() -> None:
    """Run one ingestion pass using the configured data source."""
    from market_risk.config import Settings
    from market_risk.database.engine import Base, get_engine, get_session_factory
    from market_risk.ingestion import get_data_source

    settings = Settings()
    engine = get_engine(settings.database_url)
    Base.metadata.create_all(bind=engine)
    session = get_session_factory(engine)()

    try:
        orchestrator = PipelineOrchestrator(
            source=get_data_source(settings),
            repository=MarketDataRepository(session),
        )
        result = orchestrator.run()
    finally:
        session.close()
        engine.dispose()

    print(f"Rows ingested:      {result.rows_ingested}")
    print(f"Validation errors:  {result.validation_errors}")
    print(f"Tickers processed:  {', '.join(result.tickers_processed) or '(none)'}")
    print(f"Finished at:        {result.finished_at:%Y-%m-%d %H:%M:%S %Z}")


if __name__ == "__main__":
    main()
