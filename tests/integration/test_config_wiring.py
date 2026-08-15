"""Settings must actually reach the code that depends on them."""

import pytest
from fastapi.testclient import TestClient

from market_risk.api.app import create_app
from market_risk.config import Settings
from market_risk.ingestion import get_data_source
from market_risk.ingestion.local_source import LocalSource
from market_risk.ingestion.s3_source import S3Source
from market_risk.ingestion.yahoo_source import YahooFinanceSource

CSV = (
    "ticker,date,open,high,low,close,volume\n"
    "AAPL,2024-01-02,100.0,101.0,99.0,100.0,1000000\n"
    "AAPL,2024-01-03,100.0,102.0,99.5,101.0,1000000\n"
    "AAPL,2024-01-04,101.0,103.0,100.0,102.0,1000000\n"
    "AAPL,2024-01-05,102.0,104.0,101.0,103.0,1000000\n"
)


def clean_settings(**overrides) -> Settings:
    """Settings built from declared defaults only, ignoring any local .env."""
    return Settings(_env_file=None, **overrides)


def build_client(tmp_path, **overrides):
    (tmp_path / "market_data.csv").write_text(CSV)
    settings = Settings(
        database_url=f"sqlite:///{tmp_path}/test.db",
        data_source="local",
        local_data_path=str(tmp_path),
        redis_url=None,
        **overrides,
    )
    return TestClient(create_app(settings))


class TestCacheTtlFromSettings:
    def test_route_uses_configured_ttl(self, tmp_path):
        recorded: dict[str, int] = {}

        with build_client(tmp_path, cache_ttl_seconds=987) as client:
            cache = client.app.state.cache
            original_set = cache.set

            async def spy(key: str, value: str, ttl: int) -> None:
                recorded["ttl"] = ttl
                await original_set(key, value, ttl)

            cache.set = spy  # type: ignore[method-assign]

            client.post("/api/v1/ingest/")
            assert client.get("/api/v1/metrics/AAPL").status_code == 200

        assert recorded["ttl"] == 987

    def test_ttl_default_is_300(self):
        assert clean_settings().cache_ttl_seconds == 300


class TestLastRunPopulated:
    def test_ingest_reports_last_run(self, tmp_path):
        with build_client(tmp_path) as client:
            payload = client.post("/api/v1/ingest/").json()

        assert payload["last_run"] is not None

    def test_last_run_present_even_with_no_files(self, tmp_path):
        empty = tmp_path / "empty"
        empty.mkdir()
        settings = Settings(
            database_url=f"sqlite:///{tmp_path}/test.db",
            data_source="local",
            local_data_path=str(empty),
            redis_url=None,
        )
        with TestClient(create_app(settings)) as client:
            payload = client.post("/api/v1/ingest/").json()

        assert payload["rows_ingested"] == 0
        assert payload["last_run"] is not None


class TestDataSourceSelection:
    def test_local_is_default(self):
        assert isinstance(get_data_source(clean_settings()), LocalSource)

    def test_yahoo_selected_and_tickers_parsed(self):
        source = get_data_source(
            clean_settings(data_source="yahoo", yahoo_tickers="aapl, msft")
        )
        assert isinstance(source, YahooFinanceSource)
        assert source.tickers == ["AAPL", "MSFT"]

    def test_s3_selected(self):
        source = get_data_source(clean_settings(data_source="s3", s3_bucket="bucket"))
        assert isinstance(source, S3Source)
        assert source.bucket == "bucket"

    def test_invalid_data_source_is_rejected(self):
        """A typo in MR_DATA_SOURCE should fail loudly, not fall back silently."""
        with pytest.raises(ValueError):
            clean_settings(data_source="locl")  # type: ignore[arg-type]

    def test_legacy_use_local_source_is_gone(self):
        assert not hasattr(clean_settings(), "use_local_source")
