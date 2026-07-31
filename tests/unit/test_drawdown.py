import pandas as pd
import pytest

from market_risk.metrics.drawdown import max_drawdown


class TestMaxDrawdown:
    def test_no_drawdown_monotonic_increase(self):
        prices = pd.Series([100.0, 101.0, 102.0, 103.0, 104.0])
        assert max_drawdown(prices) == 0.0

    def test_known_drawdown(self):
        prices = pd.Series([100.0, 110.0, 88.0, 95.0])
        dd = max_drawdown(prices)
        assert pytest.approx(dd, rel=1e-6) == (110.0 - 88.0) / 110.0

    def test_drawdown_at_end(self):
        prices = pd.Series([100.0, 120.0, 90.0])
        dd = max_drawdown(prices)
        assert pytest.approx(dd, rel=1e-6) == (120.0 - 90.0) / 120.0

    def test_empty_series(self):
        prices = pd.Series([], dtype=float)
        assert max_drawdown(prices) == 0.0

    def test_drawdown_between_0_and_1(self, sample_prices):
        dd = max_drawdown(sample_prices)
        assert 0.0 <= dd <= 1.0
