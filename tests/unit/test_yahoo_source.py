from unittest.mock import patch

import pandas as pd
import pytest

from market_risk.ingestion.yahoo_source import YahooFinanceSource


@pytest.fixture
def mock_yahoo_data():
    return pd.DataFrame({
        "Date": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
        "Open": [185.50, 186.00, 187.00],
        "High": [186.20, 187.10, 188.00],
        "Low": [184.80, 185.00, 186.00],
        "Close": [185.90, 186.50, 187.50],
        "Volume": [50000000, 48000000, 52000000],
    }).set_index("Date")


class TestYahooFinanceSource:
    def test_list_files_returns_tickers(self):
        source = YahooFinanceSource(tickers=["AAPL", "MSFT", "GOOGL"])
        assert source.list_files() == ["AAPL", "MSFT", "GOOGL"]

    def test_list_files_with_prefix_filter(self):
        source = YahooFinanceSource(tickers=["AAPL", "AMZN", "MSFT"])
        assert source.list_files(prefix="A") == ["AAPL", "AMZN"]

    def test_tickers_are_uppercased(self):
        source = YahooFinanceSource(tickers=["aapl", "msft"])
        assert source.list_files() == ["AAPL", "MSFT"]

    @patch("market_risk.ingestion.yahoo_source.yf.download")
    def test_read_file_returns_formatted_dataframe(self, mock_download, mock_yahoo_data):
        mock_download.return_value = mock_yahoo_data
        source = YahooFinanceSource(tickers=["AAPL"], period_days=30)

        df = source.read_file("AAPL")

        assert list(df.columns) == ["ticker", "date", "open", "high", "low", "close", "volume"]
        assert len(df) == 3
        assert all(df["ticker"] == "AAPL")
        assert df["volume"].dtype == int

    @patch("market_risk.ingestion.yahoo_source.yf.download")
    def test_read_file_empty_response(self, mock_download):
        mock_download.return_value = pd.DataFrame()
        source = YahooFinanceSource(tickers=["INVALID"], period_days=30)

        df = source.read_file("INVALID")

        assert df.empty
        assert list(df.columns) == ["ticker", "date", "open", "high", "low", "close", "volume"]

    @patch("market_risk.ingestion.yahoo_source.yf.download")
    def test_read_file_handles_multiindex_columns(self, mock_download, mock_yahoo_data):
        multi_idx = pd.MultiIndex.from_tuples(
            [(col, "AAPL") for col in mock_yahoo_data.columns],
            names=["Price", "Ticker"],
        )
        multi_df = mock_yahoo_data.copy()
        multi_df.columns = multi_idx
        mock_download.return_value = multi_df

        source = YahooFinanceSource(tickers=["AAPL"], period_days=30)
        df = source.read_file("AAPL")

        assert len(df) == 3
        assert "ticker" in df.columns
