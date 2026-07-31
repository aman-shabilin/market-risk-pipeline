import math
from dataclasses import dataclass, field
from datetime import datetime, timezone

import pandas as pd
from pydantic import ValidationError

from market_risk.database.models import ComputedMetric
from market_risk.database.repository import MarketDataRepository
from market_risk.ingestion.base import DataSource
from market_risk.metrics import (
    annualized_volatility,
    compute_daily_returns,
    conditional_var,
    historical_var,
    max_drawdown,
    sharpe_ratio,
)
from market_risk.schemas.market_data import MarketDataRow


@dataclass
class PipelineResult:
    rows_ingested: int = 0
    validation_errors: int = 0
    tickers_processed: list[str] = field(default_factory=list)
    error_details: list[str] = field(default_factory=list)


class PipelineOrchestrator:
    def __init__(self, source: DataSource, repository: MarketDataRepository):
        self.source = source
        self.repository = repository

    def run(self, ticker_filter: str | None = None) -> PipelineResult:
        result = PipelineResult()

        files = self.source.list_files()
        if not files:
            return result

        all_records: list[dict] = []
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

        return result

    def _validate_dataframe(self, df: pd.DataFrame) -> tuple[list[dict], int]:
        records: list[dict] = []
        errors = 0
        for _, row in df.iterrows():
            try:
                validated = MarketDataRow.model_validate(row.to_dict())
                records.append(validated.model_dump())
            except ValidationError:
                errors += 1
        return records, errors

    def _compute_and_store_metrics(self, ticker: str) -> None:
        prices_rows = self.repository.get_prices(ticker)
        if len(prices_rows) < 3:
            return

        prices = pd.Series([p.close for p in prices_rows])
        dates = [p.date for p in prices_rows]
        returns = compute_daily_returns(prices)

        vol = annualized_volatility(returns)
        var95 = historical_var(returns, confidence=0.95)
        var99 = historical_var(returns, confidence=0.99)
        cvar95 = conditional_var(returns, confidence=0.95)
        cvar99 = conditional_var(returns, confidence=0.99)
        sr = sharpe_ratio(returns)
        dd = max_drawdown(prices)

        if any(math.isnan(v) for v in [vol, var95, var99, cvar95, cvar99, sr, dd]):
            return

        metric = ComputedMetric(
            ticker=ticker,
            computed_at=datetime.now(timezone.utc),
            window_start=dates[0],
            window_end=dates[-1],
            annualized_volatility=vol,
            var_95=var95,
            var_99=var99,
            cvar_95=cvar95,
            cvar_99=cvar99,
            sharpe_ratio=sr,
            max_drawdown=dd,
        )
        self.repository.save_metrics(metric)
