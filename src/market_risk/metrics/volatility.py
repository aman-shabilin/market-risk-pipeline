import numpy as np
import pandas as pd

TRADING_DAYS_PER_YEAR = 252


def rolling_volatility(returns: pd.Series, window: int = 21) -> pd.Series:
    """Compute rolling annualized volatility."""
    return returns.rolling(window=window).std() * np.sqrt(TRADING_DAYS_PER_YEAR)


def annualized_volatility(returns: pd.Series) -> float:
    """Compute annualized volatility over the full series."""
    vol = float(returns.std() * np.sqrt(TRADING_DAYS_PER_YEAR))
    if vol < 1e-10:
        return 0.0
    return vol
