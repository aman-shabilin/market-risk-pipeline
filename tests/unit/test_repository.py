from datetime import date


class TestMarketDataRepository:
    def test_upsert_prices(self, repository):
        records = [
            {
                "ticker": "AAPL",
                "date": date(2024, 1, 2),
                "open": 185.5,
                "high": 186.2,
                "low": 184.8,
                "close": 185.9,
                "volume": 50000000,
            }
        ]
        count = repository.upsert_prices(records)
        assert count == 1

    def test_upsert_idempotent(self, repository):
        records = [
            {
                "ticker": "AAPL",
                "date": date(2024, 1, 2),
                "open": 185.5,
                "high": 186.2,
                "low": 184.8,
                "close": 185.9,
                "volume": 50000000,
            }
        ]
        repository.upsert_prices(records)
        count = repository.upsert_prices(records)
        assert count == 0

    def test_get_prices_filtered(self, repository):
        records = [
            {
                "ticker": "AAPL",
                "date": date(2024, 1, i),
                "open": 185.0,
                "high": 186.0,
                "low": 184.0,
                "close": 185.0 + i,
                "volume": 50000000,
            }
            for i in range(2, 12)
        ]
        repository.upsert_prices(records)
        prices = repository.get_prices("AAPL", start=date(2024, 1, 5), end=date(2024, 1, 8))
        assert len(prices) == 4
        assert all(p.ticker == "AAPL" for p in prices)

    def test_list_tickers(self, repository):
        records = [
            {
                "ticker": "AAPL", "date": date(2024, 1, 2),
                "open": 185.5, "high": 186.2, "low": 184.8,
                "close": 185.9, "volume": 50000000,
            },
            {
                "ticker": "MSFT", "date": date(2024, 1, 2),
                "open": 372.5, "high": 374.8, "low": 371.2,
                "close": 373.9, "volume": 28000000,
            },
        ]
        repository.upsert_prices(records)
        tickers = repository.list_tickers()
        assert tickers == ["AAPL", "MSFT"]

    def test_upsert_empty_list(self, repository):
        count = repository.upsert_prices([])
        assert count == 0
