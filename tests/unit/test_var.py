import numpy as np
import pandas as pd
import pytest

from market_risk.metrics.var import historical_var, parametric_var


class TestHistoricalVar:
    def test_positive_var_for_mixed_returns(self, sample_returns):
        var = historical_var(sample_returns, confidence=0.95)
        assert var > 0 or var == 0  # can be 0 if all returns positive

    def test_higher_confidence_higher_var(self):
        returns = pd.Series(np.random.normal(-0.001, 0.02, 1000))
        var_95 = historical_var(returns, confidence=0.95)
        var_99 = historical_var(returns, confidence=0.99)
        assert var_99 >= var_95

    def test_empty_returns(self):
        returns = pd.Series([], dtype=float)
        assert historical_var(returns) == 0.0

    def test_all_positive_returns(self):
        returns = pd.Series([0.01, 0.02, 0.03, 0.01, 0.02])
        var = historical_var(returns, confidence=0.95)
        assert var <= 0  # no loss at 95% confidence when all returns positive


class TestParametricVar:
    def test_parametric_vs_historical_same_order(self):
        np.random.seed(42)
        returns = pd.Series(np.random.normal(-0.001, 0.02, 10000))
        h_var = historical_var(returns, confidence=0.95)
        p_var = parametric_var(returns, confidence=0.95)
        assert abs(h_var - p_var) < 0.01

    def test_empty_returns(self):
        returns = pd.Series([], dtype=float)
        assert parametric_var(returns) == 0.0
