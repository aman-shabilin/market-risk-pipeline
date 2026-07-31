import numpy as np
import pandas as pd


def historical_var(returns: pd.Series, confidence: float = 0.95) -> float:
    """Compute Value-at-Risk using the historical simulation method.

    Returns the loss threshold (as a positive number) at the given confidence level.
    """
    if len(returns) == 0:
        return 0.0
    percentile = (1 - confidence) * 100
    return float(-np.percentile(returns, percentile))


def parametric_var(returns: pd.Series, confidence: float = 0.95) -> float:
    """Compute Value-at-Risk assuming normal distribution of returns."""
    from scipy.stats import norm

    if len(returns) == 0:
        return 0.0
    mu = returns.mean()
    sigma = returns.std()
    z_score = norm.ppf(1 - confidence)
    return float(-(mu + z_score * sigma))
