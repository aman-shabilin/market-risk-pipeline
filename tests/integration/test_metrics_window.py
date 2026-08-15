"""Tests for date-windowed metrics on GET /api/v1/metrics/{ticker}.

The window params were previously accepted, folded into the cache key, then
ignored -- so a filtered request silently returned full-history numbers.
"""

import pytest
from fastapi.testclient import TestClient

from market_risk.api.app import create_app
from market_risk.config import Settings

# A calm first half followed by a sharp selloff, so a windowed request must
# produce visibly different numbers from the full history.
VOLATILE_CSV = (
    "ticker,date,open,high,low,close,volume\n"
    "AAPL,2024-01-02,100.0,100.5,99.5,100.0,1000000\n"
    "AAPL,2024-01-03,100.0,101.0,99.8,100.5,1000000\n"
    "AAPL,2024-01-04,100.5,101.5,100.2,101.0,1000000\n"
    "AAPL,2024-01-05,101.0,102.0,100.8,101.5,1000000\n"
    "AAPL,2024-01-08,101.5,102.5,101.2,102.0,1000000\n"
    "AAPL,2024-01-09,102.0,102.5,94.0,95.0,5000000\n"
    "AAPL,2024-01-10,95.0,96.0,86.0,87.0,6000000\n"
    "AAPL,2024-01-11,87.0,90.0,84.0,89.0,5500000\n"
    "AAPL,2024-01-12,89.0,93.0,88.0,92.0,4000000\n"
    "AAPL,2024-01-15,92.0,95.0,91.0,94.0,3000000\n"
)

ENDPOINT = "/api/v1/metrics/AAPL"


@pytest.fixture
def client(tmp_path):
    (tmp_path / "market_data.csv").write_text(VOLATILE_CSV)
    settings = Settings(
        database_url=f"sqlite:///{tmp_path}/test.db",
        data_source="local",
        local_data_path=str(tmp_path),
        redis_url=None,
    )
    with TestClient(create_app(settings)) as c:
        c.post("/api/v1/ingest/")
        yield c


class TestWindowedMetrics:
    def test_window_narrows_reported_dates(self, client):
        response = client.get(
            ENDPOINT, params={"start_date": "2024-01-02", "end_date": "2024-01-08"}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["start_date"] == "2024-01-02"
        assert data["end_date"] == "2024-01-08"

    def test_calm_window_differs_from_full_history(self, client):
        calm = client.get(
            ENDPOINT, params={"start_date": "2024-01-02", "end_date": "2024-01-08"}
        ).json()
        full = client.get(ENDPOINT).json()

        assert calm["max_drawdown"] < full["max_drawdown"]
        assert calm["annualized_volatility"] < full["annualized_volatility"]
        assert calm != full

    def test_calm_window_has_no_drawdown(self, client):
        calm = client.get(
            ENDPOINT, params={"start_date": "2024-01-02", "end_date": "2024-01-08"}
        ).json()
        assert calm["max_drawdown"] == pytest.approx(0.0)

    def test_selloff_window_shows_large_drawdown(self, client):
        selloff = client.get(
            ENDPOINT, params={"start_date": "2024-01-08", "end_date": "2024-01-15"}
        ).json()
        # 102 down to 87 is roughly a 15% drawdown.
        assert selloff["max_drawdown"] == pytest.approx(0.147, abs=0.01)

    def test_start_date_only_is_honoured(self, client):
        response = client.get(ENDPOINT, params={"start_date": "2024-01-08"})
        assert response.status_code == 200
        assert response.json()["start_date"] == "2024-01-08"

    def test_end_date_only_is_honoured(self, client):
        response = client.get(ENDPOINT, params={"end_date": "2024-01-08"})
        assert response.status_code == 200
        assert response.json()["end_date"] == "2024-01-08"

    def test_distinct_windows_are_cached_separately(self, client):
        first = client.get(
            ENDPOINT, params={"start_date": "2024-01-02", "end_date": "2024-01-08"}
        ).json()
        second = client.get(
            ENDPOINT, params={"start_date": "2024-01-08", "end_date": "2024-01-15"}
        ).json()
        assert first != second

    def test_repeat_windowed_request_is_stable(self, client):
        params = {"start_date": "2024-01-02", "end_date": "2024-01-08"}
        assert client.get(ENDPOINT, params=params).json() == (
            client.get(ENDPOINT, params=params).json()
        )


class TestWindowedMetricsErrors:
    def test_window_with_no_data_returns_404(self, client):
        response = client.get(
            ENDPOINT, params={"start_date": "2030-01-01", "end_date": "2030-12-31"}
        )
        assert response.status_code == 404

    def test_window_with_too_few_points_returns_422(self, client):
        response = client.get(
            ENDPOINT, params={"start_date": "2024-01-02", "end_date": "2024-01-03"}
        )
        assert response.status_code == 422
        assert "at least" in response.json()["detail"]

    def test_inverted_window_returns_422(self, client):
        response = client.get(
            ENDPOINT, params={"start_date": "2024-01-15", "end_date": "2024-01-02"}
        )
        assert response.status_code == 422

    def test_malformed_date_returns_422(self, client):
        response = client.get(ENDPOINT, params={"start_date": "not-a-date"})
        assert response.status_code == 422

    def test_unknown_ticker_with_window_returns_404(self, client):
        response = client.get(
            "/api/v1/metrics/NOPE", params={"start_date": "2024-01-02"}
        )
        assert response.status_code == 404


class TestTickerCaseHandling:
    def test_lowercase_ticker_resolves(self, client):
        response = client.get(
            "/api/v1/metrics/aapl", params={"start_date": "2024-01-02"}
        )
        assert response.status_code == 200
        assert response.json()["ticker"] == "AAPL"
