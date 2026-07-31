import pandas as pd
import pytest

from market_risk.metrics.cvar import conditional_var


class TestConditionalVar:
    def test_cvar_greater_than_or_equal_to_var(self):
        returns = pd.Series([-0.05, -0.03, -0.01, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07])
        from market_risk.metrics.var import historical_var

        var95 = historical_var(returns, confidence=0.95)
        cvar95 = conditional_var(returns, confidence=0.95)
        assert cvar95 >= var95

    def test_cvar_positive_for_mixed_returns(self):
        returns = pd.Series([-0.05, -0.03, -0.01, 0.01, 0.02, 0.03, 0.04, 0.05])
        cvar = conditional_var(returns, confidence=0.95)
        assert cvar > 0

    def test_cvar_zero_for_all_positive_returns(self):
        returns = pd.Series([0.01, 0.02, 0.03, 0.04, 0.05])
        cvar = conditional_var(returns, confidence=0.95)
        assert cvar <= 0 or cvar == pytest.approx(0, abs=0.01)

    def test_cvar_empty_returns(self):
        returns = pd.Series([], dtype=float)
        assert conditional_var(returns, confidence=0.95) == 0.0

    def test_higher_confidence_higher_cvar(self):
        returns = pd.Series([-0.10, -0.08, -0.05, -0.03, -0.01, 0.01, 0.02, 0.03, 0.04, 0.05,
                             0.06, 0.07, 0.08, 0.09, 0.10, 0.11, 0.12, 0.13, 0.14, 0.15])
        cvar95 = conditional_var(returns, confidence=0.95)
        cvar99 = conditional_var(returns, confidence=0.99)
        assert cvar99 >= cvar95

    def test_known_value(self):
        returns = pd.Series([-0.10, -0.05, 0.0, 0.05, 0.10])
        cvar = conditional_var(returns, confidence=0.80)
        assert cvar == pytest.approx(0.10, abs=0.01)
