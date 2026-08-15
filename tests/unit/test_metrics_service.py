from datetime import date, timedelta

import pytest

from market_risk.database.models import MarketPrice
from market_risk.metrics import MIN_PRICE_POINTS, compute_risk_metrics


def price_rows(closes: list[float], ticker: str = "AAPL") -> list[MarketPrice]:
    start = date(2024, 1, 2)
    return [
        MarketPrice(
            ticker=ticker,
            date=start + timedelta(days=i),
            open=close,
            high=close * 1.01,
            low=close * 0.99,
            close=close,
            volume=1_000_000,
        )
        for i, close in enumerate(closes)
    ]


class TestComputeRiskMetrics:
    def test_returns_none_below_minimum_points(self):
        assert compute_risk_metrics("AAPL", price_rows([100.0, 101.0])) is None

    def test_returns_none_for_empty_input(self):
        assert compute_risk_metrics("AAPL", []) is None

    def test_computes_at_minimum_points(self):
        rows = price_rows([100.0, 101.0, 99.0])
        assert len(rows) == MIN_PRICE_POINTS
        assert compute_risk_metrics("AAPL", rows) is not None

    def test_window_bounds_come_from_first_and_last_row(self):
        rows = price_rows([100.0, 101.0, 99.5, 102.0, 101.0])
        metrics = compute_risk_metrics("AAPL", rows)
        assert metrics is not None
        assert metrics.window_start == rows[0].date
        assert metrics.window_end == rows[-1].date
        assert metrics.ticker == "AAPL"

    def test_flat_prices_give_zero_volatility_and_drawdown(self):
        metrics = compute_risk_metrics("AAPL", price_rows([100.0] * 10))
        assert metrics is not None
        assert metrics.annualized_volatility == 0.0
        assert metrics.max_drawdown == 0.0
        assert metrics.sharpe_ratio == 0.0

    def test_monotonic_rise_has_no_drawdown(self):
        metrics = compute_risk_metrics("AAPL", price_rows([100.0, 102.0, 105.0, 110.0]))
        assert metrics is not None
        assert metrics.max_drawdown == pytest.approx(0.0)

    def test_drawdown_matches_known_value(self):
        # Peak 120 falling to 90 is a 25% drawdown.
        metrics = compute_risk_metrics("AAPL", price_rows([100.0, 120.0, 90.0, 110.0]))
        assert metrics is not None
        assert metrics.max_drawdown == pytest.approx(0.25)

    def test_cvar_is_at_least_var(self):
        closes = [100.0, 103.0, 98.0, 105.0, 94.0, 108.0, 101.0, 112.0, 97.0, 106.0]
        metrics = compute_risk_metrics("AAPL", price_rows(closes))
        assert metrics is not None
        assert metrics.cvar_95 >= metrics.var_95
        assert metrics.cvar_99 >= metrics.var_99

    def test_var_99_is_at_least_var_95(self):
        closes = [100.0, 103.0, 98.0, 105.0, 94.0, 108.0, 101.0, 112.0, 97.0, 106.0]
        metrics = compute_risk_metrics("AAPL", price_rows(closes))
        assert metrics is not None
        assert metrics.var_99 >= metrics.var_95

    def test_all_metrics_are_finite(self):
        import math

        closes = [100.0 + (i % 7) * 3 for i in range(30)]
        metrics = compute_risk_metrics("AAPL", price_rows(closes))
        assert metrics is not None
        for field in (
            metrics.annualized_volatility,
            metrics.var_95,
            metrics.var_99,
            metrics.cvar_95,
            metrics.cvar_99,
            metrics.sharpe_ratio,
            metrics.max_drawdown,
        ):
            assert math.isfinite(field)

    def test_subset_window_differs_from_full_window(self):
        closes = [100.0, 101.0, 102.0, 103.0, 104.0, 80.0, 82.0, 84.0, 86.0, 88.0]
        rows = price_rows(closes)
        calm = compute_risk_metrics("AAPL", rows[:5])
        full = compute_risk_metrics("AAPL", rows)
        assert calm is not None and full is not None
        assert calm.max_drawdown < full.max_drawdown
        assert calm.annualized_volatility < full.annualized_volatility

    def test_result_is_immutable(self):
        metrics = compute_risk_metrics("AAPL", price_rows([100.0, 101.0, 99.0]))
        assert metrics is not None
        with pytest.raises(AttributeError):
            metrics.var_95 = 0.5  # type: ignore[misc]
