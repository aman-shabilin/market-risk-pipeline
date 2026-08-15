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
        "MSFT,2024-01-02,372.5,374.8,371.2,373.9,28000000\n"
        "MSFT,2024-01-03,373.9,375.5,372.8,374.5,26000000\n"
        "MSFT,2024-01-04,374.5,375.0,371.5,372.0,30000000\n"
        "MSFT,2024-01-05,372.0,373.8,370.5,373.2,27000000\n"
        "MSFT,2024-01-08,373.2,375.0,372.5,374.8,25000000\n"
        "MSFT,2024-01-09,374.8,376.5,374.0,376.0,29000000\n"
        "MSFT,2024-01-10,376.0,378.0,375.5,377.5,31000000\n"
        "MSFT,2024-01-11,377.5,379.0,377.0,378.5,28000000\n"
        "MSFT,2024-01-12,378.5,380.0,378.0,379.8,26000000\n"
        "MSFT,2024-01-16,379.8,381.0,379.0,380.5,27000000\n"
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


class TestPortfolioEndpoints:
    def test_create_portfolio(self, client):
        response = client.post("/api/v1/portfolios/", json={
            "name": "tech_blend",
            "holdings": [
                {"ticker": "AAPL", "weight": 0.6},
                {"ticker": "MSFT", "weight": 0.4},
            ],
        })
        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "tech_blend"
        assert len(data["holdings"]) == 2
        weights = {h["ticker"]: h["weight"] for h in data["holdings"]}
        assert weights["AAPL"] == pytest.approx(0.6, rel=1e-3)
        assert weights["MSFT"] == pytest.approx(0.4, rel=1e-3)

    def test_create_duplicate_portfolio_fails(self, client):
        client.post("/api/v1/portfolios/", json={
            "name": "dupe",
            "holdings": [{"ticker": "AAPL", "weight": 1.0}],
        })
        response = client.post("/api/v1/portfolios/", json={
            "name": "dupe",
            "holdings": [{"ticker": "MSFT", "weight": 1.0}],
        })
        assert response.status_code == 409

    def test_list_portfolios(self, client):
        client.post("/api/v1/portfolios/", json={
            "name": "p1",
            "holdings": [{"ticker": "AAPL", "weight": 1.0}],
        })
        response = client.get("/api/v1/portfolios/")
        assert response.status_code == 200
        assert len(response.json()) >= 1

    def test_get_portfolio_risk(self, client):
        client.post("/api/v1/ingest/")
        client.post("/api/v1/portfolios/", json={
            "name": "balanced",
            "holdings": [
                {"ticker": "AAPL", "weight": 0.5},
                {"ticker": "MSFT", "weight": 0.5},
            ],
        })
        response = client.get("/api/v1/portfolios/balanced/risk")
        assert response.status_code == 200
        data = response.json()
        assert data["portfolio_name"] == "balanced"
        assert data["annualized_volatility"] > 0
        assert data["var_95"] > 0
        assert data["cvar_95"] >= data["var_95"]
        assert data["diversification_ratio"] >= 1.0
        assert data["max_drawdown"] >= 0

    def test_portfolio_risk_not_found(self, client):
        response = client.get("/api/v1/portfolios/nonexistent/risk")
        assert response.status_code == 404

    def test_portfolio_risk_without_data(self, client):
        client.post("/api/v1/portfolios/", json={
            "name": "empty_data",
            "holdings": [{"ticker": "ZZZZ", "weight": 1.0}],
        })
        response = client.get("/api/v1/portfolios/empty_data/risk")
        assert response.status_code == 404
