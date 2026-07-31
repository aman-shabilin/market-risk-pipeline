import pandas as pd


def compute_daily_returns(prices: pd.Series) -> pd.Series:
    """Compute simple daily returns from a price series."""
    returns = prices.pct_change().dropna()
    return returns
