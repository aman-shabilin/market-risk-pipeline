from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "sqlite:///./market_risk.db"

    s3_bucket: str = ""
    s3_prefix: str = "market-data/"
    aws_region: str = "us-east-1"

    use_local_source: bool = True
    local_data_path: str = "./data/sample"

    data_source: str = "local"  # "local", "s3", or "yahoo"
    yahoo_tickers: str = "AAPL,MSFT,GOOGL"
    yahoo_period_days: int = 365

    redis_url: str | None = None
    cache_ttl_seconds: int = 300

    api_host: str = "0.0.0.0"
    api_port: int = 8000

    model_config = {"env_prefix": "MR_", "env_file": ".env"}
