import pandas as pd
import pytest

from market_risk.metrics.returns import compute_daily_returns


class TestComputeDailyReturns:
    def test_basic_returns(self):
        prices = pd.Series([100.0, 110.0, 99.0])
        returns = compute_daily_returns(prices)
        assert len(returns) == 2
        assert pytest.approx(returns.iloc[0], rel=1e-6) == 0.1
        assert pytest.approx(returns.iloc[1], rel=1e-6) == -0.1

    def test_constant_prices(self):
        prices = pd.Series([50.0, 50.0, 50.0, 50.0])
        returns = compute_daily_returns(prices)
        assert all(r == 0.0 for r in returns)

    def test_single_price(self):
        prices = pd.Series([100.0])
        returns = compute_daily_returns(prices)
        assert len(returns) == 0

    def test_drops_na(self):
        prices = pd.Series([100.0, 105.0, 110.0])
        returns = compute_daily_returns(prices)
        assert not returns.isna().any()
