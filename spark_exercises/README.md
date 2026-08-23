# PySpark Data Cleansing Exercises

Each level teaches one transformation concept. Run them in order.

## Setup
```bash
pip install pyspark
```

## The dirty data has these problems:
1. Mixed case tickers ("aapl" vs "AAPL")
2. Whitespace in tickers (" AAPL ")
3. Negative volume
4. High < Low (impossible candle)
5. Exact duplicate rows
6. Duplicate (ticker, date) with different values
7. Null/missing values in critical columns
8. Non-numeric values in numeric columns ("fifty_million", "not_a_number")
9. Future dates (2099)
10. Zero-price rows (likely bad data)
11. Missing ticker or date entirely
12. Outlier values (999999.99 high)

## Levels
- Level 1: Read, inspect, basic selects
- Level 2: Standardization (case, trim, types)
- Level 3: Null handling strategies
- Level 4: Filtering invalid records
- Level 5: Deduplication strategies
- Level 6: Derived columns and window functions
- Level 7: Aggregations and joins
- Level 8: Full pipeline — chain everything together
