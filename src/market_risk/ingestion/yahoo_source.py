from datetime import date, timedelta

import pandas as pd
import yfinance as yf

from market_risk.ingestion.base import DataSource


class YahooFinanceSource(DataSource):
    """Fetch OHLCV data from Yahoo Finance for a list of tickers."""

    def __init__(self, tickers: list[str], period_days: int = 365):
        self.tickers = [t.upper() for t in tickers]
        self.period_days = period_days

    def list_files(self, prefix: str = "") -> list[str]:
        if prefix:
            return [t for t in self.tickers if t.startswith(prefix.upper())]
        return list(self.tickers)

    def read_file(self, path: str) -> pd.DataFrame:
        ticker = path.upper()
        end = date.today()
        start = end - timedelta(days=self.period_days)

        raw = yf.download(
            ticker,
            start=start.isoformat(),
            end=end.isoformat(),
            progress=False,
            auto_adjust=True,
        )

        if raw.empty:
            cols = ["ticker", "date", "open", "high", "low", "close", "volume"]
            return pd.DataFrame(columns=cols)

        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)

        df = raw.reset_index()
        df = df.rename(columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        })
        df["ticker"] = ticker
        df["volume"] = df["volume"].astype(int)
        return df[["ticker", "date", "open", "high", "low", "close", "volume"]]
