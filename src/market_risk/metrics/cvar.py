import numpy as np
import pandas as pd


def conditional_var(returns: pd.Series, confidence: float = 0.95) -> float:
    """Compute Conditional Value-at-Risk (Expected Shortfall).

    CVaR is the average loss in the worst (1 - confidence) tail.
    Returns a positive number representing the expected loss magnitude.
    """
    if len(returns) == 0:
        return 0.0
    percentile = (1 - confidence) * 100
    var_threshold = np.percentile(returns, percentile)
    tail_losses = returns[returns <= var_threshold]
    if len(tail_losses) == 0:
        return 0.0
    return float(-tail_losses.mean())
