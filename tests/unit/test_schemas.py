from datetime import date

import pytest
from pydantic import ValidationError

from market_risk.schemas.market_data import MarketDataRow


class TestMarketDataRow:
    def test_valid_row(self):
        row = MarketDataRow(
            ticker="aapl",
            date=date(2024, 1, 2),
            open=185.5,
            high=186.2,
            low=184.8,
            close=185.9,
            volume=50000000,
        )
        assert row.ticker == "AAPL"

    def test_ticker_uppercased(self):
        row = MarketDataRow(
            ticker="  msft  ",
            date=date(2024, 1, 2),
            open=372.5,
            high=374.8,
            low=371.2,
            close=373.9,
            volume=28000000,
        )
        assert row.ticker == "MSFT"

    def test_negative_volume_rejected(self):
        with pytest.raises(ValidationError, match="volume must be non-negative"):
            MarketDataRow(
                ticker="AAPL",
                date=date(2024, 1, 2),
                open=185.5,
                high=186.2,
                low=184.8,
                close=185.9,
                volume=-100,
            )

    def test_high_less_than_low_rejected(self):
        with pytest.raises(ValidationError, match="high must be >= low"):
            MarketDataRow(
                ticker="AAPL",
                date=date(2024, 1, 2),
                open=185.5,
                high=180.0,
                low=184.8,
                close=185.9,
                volume=50000000,
            )

    def test_date_parsing(self):
        row = MarketDataRow(
            ticker="AAPL",
            date="2024-01-02",
            open=185.5,
            high=186.2,
            low=184.8,
            close=185.9,
            volume=50000000,
        )
        assert row.date == date(2024, 1, 2)
