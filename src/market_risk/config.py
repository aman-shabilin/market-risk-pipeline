from typing import Literal

from pydantic_settings import BaseSettings

DataSourceName = Literal["local", "s3", "yahoo"]


class Settings(BaseSettings):
    database_url: str = "sqlite:///./market_risk.db"

    data_source: DataSourceName = "local"

    local_data_path: str = "./data/sample"

    s3_bucket: str = ""
    s3_prefix: str = "market-data/"
    aws_region: str = "us-east-1"

    yahoo_tickers: str = "AAPL,MSFT,GOOGL"
    yahoo_period_days: int = 365

    redis_url: str | None = None
    cache_ttl_seconds: int = 300

    risk_free_rate: float = 0.02

    api_host: str = "0.0.0.0"
    api_port: int = 8000

    model_config = {"env_prefix": "MR_", "env_file": ".env"}
