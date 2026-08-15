from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class PortfolioRiskResult:
    annualized_volatility: float
    var_95: float
    var_99: float
    cvar_95: float
    cvar_99: float
    sharpe_ratio: float
    max_drawdown: float
    diversification_ratio: float


def compute_portfolio_risk(
    returns_by_ticker: dict[str, pd.Series],
    weights: dict[str, float],
    risk_free_rate: float = 0.0,
) -> PortfolioRiskResult | None:
    """Compute portfolio-level risk metrics from individual ticker returns.

    weights should sum to 1.0 (fully invested portfolio).
    """
    tickers = [t for t in weights if t in returns_by_ticker]
    if len(tickers) < 1:
        return None

    returns_df = pd.DataFrame({t: returns_by_ticker[t] for t in tickers}).dropna()
    if len(returns_df) < 3:
        return None

    w = np.array([weights[t] for t in tickers])
    w = w / w.sum()

    portfolio_returns = returns_df.values @ w

    port_vol = float(np.std(portfolio_returns, ddof=1) * np.sqrt(252))

    individual_vols = np.array([
        float(returns_df[t].std(ddof=1) * np.sqrt(252)) for t in tickers
    ])
    weighted_avg_vol = float(np.dot(w, individual_vols))
    diversification_ratio = weighted_avg_vol / port_vol if port_vol > 0 else 1.0

    var_95 = float(-np.percentile(portfolio_returns, 5))
    var_99 = float(-np.percentile(portfolio_returns, 1))

    tail_95 = portfolio_returns[portfolio_returns <= np.percentile(portfolio_returns, 5)]
    cvar_95 = float(-tail_95.mean()) if len(tail_95) > 0 else var_95

    tail_99 = portfolio_returns[portfolio_returns <= np.percentile(portfolio_returns, 1)]
    cvar_99 = float(-tail_99.mean()) if len(tail_99) > 0 else var_99

    mean_return = float(np.mean(portfolio_returns))
    std_return = float(np.std(portfolio_returns, ddof=1))
    sharpe = ((mean_return - risk_free_rate / 252) / std_return * np.sqrt(252)) if std_return > 0 else 0.0

    cumulative = np.cumprod(1 + portfolio_returns)
    running_max = np.maximum.accumulate(cumulative)
    drawdowns = (cumulative - running_max) / running_max
    max_dd = float(-drawdowns.min()) if len(drawdowns) > 0 else 0.0

    return PortfolioRiskResult(
        annualized_volatility=port_vol,
        var_95=var_95,
        var_99=var_99,
        cvar_95=cvar_95,
        cvar_99=cvar_99,
        sharpe_ratio=sharpe,
        max_drawdown=max_dd,
        diversification_ratio=diversification_ratio,
    )
