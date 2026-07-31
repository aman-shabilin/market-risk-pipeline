from market_risk.ingestion.base import DataSource
from market_risk.ingestion.local_source import LocalSource
from market_risk.ingestion.s3_source import S3Source
from market_risk.ingestion.yahoo_source import YahooFinanceSource
from market_risk.config import Settings


def get_data_source(settings: Settings) -> DataSource:
    source = settings.data_source.lower()
    if source == "yahoo":
        tickers = [t.strip() for t in settings.yahoo_tickers.split(",") if t.strip()]
        return YahooFinanceSource(tickers=tickers, period_days=settings.yahoo_period_days)
    if source == "s3":
        return S3Source(
            bucket=settings.s3_bucket,
            prefix=settings.s3_prefix,
            region=settings.aws_region,
        )
    return LocalSource(settings.local_data_path)


__all__ = ["DataSource", "LocalSource", "S3Source", "YahooFinanceSource", "get_data_source"]
