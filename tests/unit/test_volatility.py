import numpy as np
import pandas as pd
import pytest

from market_risk.metrics.volatility import annualized_volatility, rolling_volatility


class TestAnnualizedVolatility:
    def test_positive_volatility(self, sample_returns):
        vol = annualized_volatility(sample_returns)
        assert vol > 0

    def test_zero_volatility_for_constant_returns(self):
        returns = pd.Series([0.01] * 20)
        vol = annualized_volatility(returns)
        assert vol == 0.0

    def test_higher_vol_for_more_volatile_series(self):
        calm = pd.Series([0.001, -0.001] * 50)
        wild = pd.Series([0.05, -0.05] * 50)
        assert annualized_volatility(calm) < annualized_volatility(wild)

    def test_annualization_factor(self):
        returns = pd.Series(np.random.normal(0, 0.01, 252))
        vol = annualized_volatility(returns)
        daily_std = returns.std()
        expected = daily_std * np.sqrt(252)
        assert pytest.approx(vol, rel=1e-6) == expected


class TestRollingVolatility:
    def test_output_length(self, sample_returns):
        rolling = rolling_volatility(sample_returns, window=5)
        assert len(rolling) == len(sample_returns)

    def test_nan_at_start(self, sample_returns):
        rolling = rolling_volatility(sample_returns, window=5)
        assert rolling.iloc[:4].isna().all()
        assert not rolling.iloc[4:].isna().any()
