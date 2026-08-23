"""
Level 7: Aggregations and Joins
================================
Goal: Summarize data and combine datasets — the bread and butter of analytics.

Key PySpark concepts:
- .groupBy().agg()
- F.avg(), F.sum(), F.min(), F.max(), F.count(), F.stddev()
- .join() — inner, left, anti
- Creating reference/lookup DataFrames
- .pivot() — reshape long to wide
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
spark = SparkSession.builder.appName("level-7").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.csv("data/dirty/market_data_dirty.csv", header=True, inferSchema=True)

# Full clean pipeline (levels 2-5 condensed)
window_dedup = Window.partitionBy("ticker", "date").orderBy(F.col("volume").desc())
df = (
    df
    .withColumn("ticker", F.upper(F.trim(F.col("ticker"))))
    .withColumn("date", F.col("date").cast("date"))
    .withColumn("open", F.expr("try_cast(open as DOUBLE)"))
    .withColumn("high", F.expr("try_cast(high as DOUBLE)"))
    .withColumn("low", F.expr("try_cast(low as DOUBLE)"))
    .withColumn("close", F.expr("try_cast(close as DOUBLE)"))
    .withColumn("volume", F.expr("try_cast(volume as INT)"))
    .na.drop(subset=["ticker", "date", "close"])
    .withColumn("open", F.coalesce(F.col("open"), F.col("close")))
    .na.fill({"volume": 0})
    .filter(F.col("volume") >= 0)
    .filter(F.col("high") >= F.col("low"))
    .filter(F.col("date") <= F.current_date())
    .filter(~((F.col("open") == 0) & (F.col("close") == 0)))
    .filter(F.col("high") <= F.col("close") * 10)
    .dropDuplicates()
    .withColumn("_rn", F.row_number().over(window_dedup))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# Add daily returns
ticker_window = Window.partitionBy("ticker").orderBy("date")
df = df.withColumn("prev_close", F.lag("close", 1).over(ticker_window))
df = df.withColumn(
    "daily_return",
    F.when(
        F.col("prev_close").isNotNull() & (F.col("prev_close") > 0),
        (F.col("close") - F.col("prev_close")) / F.col("prev_close")
    )
).drop("prev_close")

# ==========================================================================
# AGGREGATION 1: Summary stats per ticker
# ==========================================================================
print("=== Per-ticker summary ===")
summary = df.groupBy("ticker").agg(
    F.count("*").alias("trading_days"),
    F.min("date").alias("first_date"),
    F.max("date").alias("last_date"),
    F.round(F.avg("close"), 2).alias("avg_close"),
    F.round(F.min("close"), 2).alias("min_close"),
    F.round(F.max("close"), 2).alias("max_close"),
    F.round(F.avg("volume"), 0).alias("avg_volume"),
    F.round(F.stddev("daily_return"), 6).alias("return_volatility"),
)
summary.show(truncate=False)

# ==========================================================================
# AGGREGATION 2: Weekly OHLCV bars (resample daily → weekly)
# ==========================================================================
print("=== Weekly bars ===")
df_with_week = df.withColumn("week_start", F.date_trunc("week", F.col("date")))

weekly = df_with_week.groupBy("ticker", "week_start").agg(
    F.first("open").alias("open"),
    F.max("high").alias("high"),
    F.min("low").alias("low"),
    F.last("close").alias("close"),
    F.sum("volume").alias("volume"),
)
weekly.orderBy("ticker", "week_start").show(truncate=False)

# ==========================================================================
# AGGREGATION 3: JOIN with a reference table
# ==========================================================================
print("=== Join with sector lookup ===")

# Create a reference DataFrame (in real life this comes from another table/file)
sectors = spark.createDataFrame([
    ("AAPL", "Technology", 3000000000000),
    ("MSFT", "Technology", 2800000000000),
    ("GOOGL", "Technology", 1900000000000),
    ("JPM", "Finance", 500000000000),
], ["ticker", "sector", "market_cap"])

# LEFT JOIN: keep all price data, attach sector info where available
df_enriched = df.join(sectors, on="ticker", how="left")
df_enriched.select("ticker", "date", "close", "sector", "market_cap").show(10, truncate=False)

# ANTI JOIN: find tickers in our price data that are NOT in the reference table
# (data quality check: do we have unknown tickers?)
unknown_tickers = df.select("ticker").distinct().join(sectors, on="ticker", how="anti")
print("Tickers not in our reference table:")
unknown_tickers.show()

# ==========================================================================
# AGGREGATION 4: PIVOT — reshape for comparison
# ==========================================================================
print("=== Pivot: daily close by ticker (wide format) ===")
pivot_df = (
    df.select("ticker", "date", "close")
    .groupBy("date")
    .pivot("ticker")
    .agg(F.first("close"))
    .orderBy("date")
)
pivot_df.show(truncate=False)

spark.stop()

# =============================================================================
# WHAT YOU LEARNED:
# - groupBy().agg() is SQL's GROUP BY — summarize many rows into one
# - F.first()/F.last() get the opening/closing value in a group
# - date_trunc("week", ...) is how you resample time series
# - LEFT join keeps all rows from the left, fills nulls where no match
# - ANTI join finds "what's missing" — great for data quality checks
# - pivot() turns row values into columns (long → wide format)
# - These patterns cover 90% of what analytics pipelines actually do
# =============================================================================
