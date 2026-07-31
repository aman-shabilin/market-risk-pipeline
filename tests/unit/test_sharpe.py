import pandas as pd

from market_risk.metrics.sharpe import sharpe_ratio


class TestSharpeRatio:
    def test_positive_sharpe_for_positive_returns(self):
        returns = pd.Series([0.01, 0.02, 0.015, 0.01, 0.02] * 50)
        sr = sharpe_ratio(returns, risk_free_rate=0.02)
        assert sr > 0

    def test_zero_sharpe_for_zero_std(self):
        returns = pd.Series([0.01] * 20)
        sr = sharpe_ratio(returns)
        assert sr == 0.0

    def test_empty_returns(self):
        returns = pd.Series([], dtype=float)
        assert sharpe_ratio(returns) == 0.0

    def test_negative_sharpe_for_underperforming(self):
        returns = pd.Series([-0.01, -0.02, -0.015, -0.01, -0.02] * 50)
        sr = sharpe_ratio(returns, risk_free_rate=0.05)
        assert sr < 0

    def test_higher_risk_free_lowers_sharpe(self):
        returns = pd.Series([0.005, 0.006, 0.004, 0.005, 0.007] * 50)
        sr_low_rf = sharpe_ratio(returns, risk_free_rate=0.01)
        sr_high_rf = sharpe_ratio(returns, risk_free_rate=0.10)
        assert sr_low_rf > sr_high_rf
