import numpy as np
import pandas as pd
import pytest

from market_risk.metrics.portfolio import compute_portfolio_risk


@pytest.fixture
def correlated_returns():
    np.random.seed(42)
    n = 100
    base = np.random.normal(0.001, 0.02, n)
    return {
        "AAPL": pd.Series(base + np.random.normal(0, 0.005, n)),
        "MSFT": pd.Series(base + np.random.normal(0, 0.005, n)),
    }


@pytest.fixture
def uncorrelated_returns():
    np.random.seed(42)
    n = 100
    return {
        "AAPL": pd.Series(np.random.normal(0.001, 0.02, n)),
        "MSFT": pd.Series(np.random.normal(0.001, 0.02, n)),
    }


class TestPortfolioRisk:
    def test_equal_weight_portfolio(self, correlated_returns):
        weights = {"AAPL": 0.5, "MSFT": 0.5}
        result = compute_portfolio_risk(correlated_returns, weights)
        assert result is not None
        assert result.annualized_volatility > 0
        assert result.var_95 > 0
        assert result.cvar_95 >= result.var_95
        assert result.max_drawdown >= 0

    def test_single_ticker_portfolio(self, correlated_returns):
        weights = {"AAPL": 1.0}
        result = compute_portfolio_risk(correlated_returns, weights)
        assert result is not None
        assert result.diversification_ratio == pytest.approx(1.0, abs=0.01)

    def test_diversification_reduces_risk(self, uncorrelated_returns):
        single = compute_portfolio_risk(uncorrelated_returns, {"AAPL": 1.0})
        diversified = compute_portfolio_risk(uncorrelated_returns, {"AAPL": 0.5, "MSFT": 0.5})
        assert diversified is not None and single is not None
        assert diversified.annualized_volatility < single.annualized_volatility

    def test_diversification_ratio_above_one_for_imperfect_correlation(self, uncorrelated_returns):
        weights = {"AAPL": 0.5, "MSFT": 0.5}
        result = compute_portfolio_risk(uncorrelated_returns, weights)
        assert result is not None
        assert result.diversification_ratio > 1.0

    def test_weights_are_normalized(self, correlated_returns):
        result_normalized = compute_portfolio_risk(correlated_returns, {"AAPL": 0.5, "MSFT": 0.5})
        result_unnormalized = compute_portfolio_risk(correlated_returns, {"AAPL": 1.0, "MSFT": 1.0})
        assert result_normalized is not None and result_unnormalized is not None
        assert result_normalized.annualized_volatility == pytest.approx(
            result_unnormalized.annualized_volatility, rel=1e-6
        )

    def test_returns_none_for_missing_tickers(self):
        result = compute_portfolio_risk({}, {"AAPL": 0.5, "MSFT": 0.5})
        assert result is None

    def test_returns_none_for_insufficient_data(self):
        short = {"AAPL": pd.Series([0.01, 0.02])}
        result = compute_portfolio_risk(short, {"AAPL": 1.0})
        assert result is None
