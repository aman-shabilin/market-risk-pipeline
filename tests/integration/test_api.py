
import pytest
from fastapi.testclient import TestClient

from market_risk.api.app import create_app
from market_risk.config import Settings


@pytest.fixture
def app_with_data(tmp_path):
    csv_content = (
        "ticker,date,open,high,low,close,volume\n"
        "AAPL,2024-01-02,185.5,186.2,184.8,185.9,50000000\n"
        "AAPL,2024-01-03,185.9,187.1,185.0,186.5,48000000\n"
        "AAPL,2024-01-04,186.5,186.8,184.2,184.5,52000000\n"
        "AAPL,2024-01-05,184.5,185.9,183.8,185.2,47000000\n"
        "AAPL,2024-01-08,185.2,186.5,184.9,186.0,45000000\n"
        "AAPL,2024-01-09,186.0,187.2,185.5,186.8,49000000\n"
        "AAPL,2024-01-10,186.8,188.0,186.2,187.5,53000000\n"
        "AAPL,2024-01-11,187.5,188.5,187.0,188.2,51000000\n"
        "AAPL,2024-01-12,188.2,189.0,187.8,188.8,46000000\n"
        "AAPL,2024-01-16,188.8,189.5,188.0,189.1,48000000\n"
    )
    (tmp_path / "market_data.csv").write_text(csv_content)

    settings = Settings(
        database_url=f"sqlite:///{tmp_path}/test.db",
        data_source="local",
        local_data_path=str(tmp_path),
        redis_url=None,
    )
    app = create_app(settings)
    return app


@pytest.fixture
def client(app_with_data):
    with TestClient(app_with_data) as c:
        yield c


class TestHealthEndpoint:
    def test_health_returns_200(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "healthy"}


class TestIngestEndpoint:
    def test_trigger_ingest(self, client):
        response = client.post("/api/v1/ingest/")
        assert response.status_code == 200
        data = response.json()
        assert data["rows_ingested"] == 10
        assert data["errors"] == 0
        assert "AAPL" in data["tickers_processed"]

    def test_ingest_with_ticker_filter(self, client):
        response = client.post("/api/v1/ingest/?ticker=AAPL")
        assert response.status_code == 200
        data = response.json()
        assert data["tickers_processed"] == ["AAPL"]


class TestMetricsEndpoint:
    def test_get_metrics_after_ingest(self, client):
        client.post("/api/v1/ingest/")
        response = client.get("/api/v1/metrics/AAPL")
        assert response.status_code == 200
        data = response.json()
        assert data["ticker"] == "AAPL"
        assert "annualized_volatility" in data
        assert "var_95" in data
        assert "sharpe_ratio" in data
        assert "max_drawdown" in data

    def test_get_metrics_not_found(self, client):
        response = client.get("/api/v1/metrics/INVALID")
        assert response.status_code == 404

    def test_list_tickers_empty(self, client):
        response = client.get("/api/v1/metrics/tickers")
        assert response.status_code == 200
        assert response.json() == []

    def test_list_tickers_after_ingest(self, client):
        client.post("/api/v1/ingest/")
        response = client.get("/api/v1/metrics/tickers")
        assert response.status_code == 200
        assert "AAPL" in response.json()

    def test_metrics_caching(self, client):
        client.post("/api/v1/ingest/")
        r1 = client.get("/api/v1/metrics/AAPL")
        r2 = client.get("/api/v1/metrics/AAPL")
        assert r1.json() == r2.json()
