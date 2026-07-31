import pandas as pd


def max_drawdown(prices: pd.Series) -> float:
    """Compute maximum drawdown from a price series.

    Returns the drawdown as a positive fraction (e.g. 0.15 = 15% drawdown).
    """
    if len(prices) == 0:
        return 0.0
    cumulative_max = prices.cummax()
    drawdowns = (cumulative_max - prices) / cumulative_max
    return float(drawdowns.max())
