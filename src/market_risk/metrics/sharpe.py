import numpy as np
import pandas as pd

TRADING_DAYS_PER_YEAR = 252


def sharpe_ratio(returns: pd.Series, risk_free_rate: float = 0.02) -> float:
    """Compute annualized Sharpe ratio.

    risk_free_rate is annualized (e.g. 0.02 = 2%).
    """
    if len(returns) == 0 or returns.std() < 1e-10:
        return 0.0
    daily_rf = risk_free_rate / TRADING_DAYS_PER_YEAR
    excess_returns = returns - daily_rf
    return float(
        excess_returns.mean() / excess_returns.std() * np.sqrt(TRADING_DAYS_PER_YEAR)
    )
