"""
Level 4: Filtering Invalid Records
===================================
Goal: Remove rows that are technically parseable but logically wrong.

Key PySpark concepts:
- .filter() / .where() (same thing)
- F.col() comparisons: >, <, >=, ==, between()
- F.current_date()
- Chaining multiple filters
- Counting what you removed (data quality metrics)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.appName("level-4").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.csv("data/dirty/market_data_dirty.csv", header=True, inferSchema=True)

# Standardize + cast (from levels 2-3)
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
)

count_before = df.count()
print(f"Rows after null handling: {count_before}")

# --- FILTER 1: Negative volume ---
# Volume can't be negative. Zero is okay (might mean no trades).
df = df.filter(F.col("volume") >= 0)
print(f"After removing negative volume: {df.count()}")

# --- FILTER 2: High must be >= Low ---
# This is a physical constraint of a price candle.
# If high < low, the data source gave us garbage.
df = df.filter(F.col("high") >= F.col("low"))
print(f"After removing high < low: {df.count()}")

# --- FILTER 3: No future dates ---
# We can't have prices from the future. Data error or placeholder.
df = df.filter(F.col("date") <= F.current_date())
print(f"After removing future dates: {df.count()}")

# --- FILTER 4: No zero-price rows ---
# A stock trading at $0.00 across all fields is almost certainly bad data.
# (Delisted stocks still have a last price > 0)
df = df.filter(
    ~((F.col("open") == 0) & (F.col("high") == 0) & (F.col("low") == 0) & (F.col("close") == 0))
)
print(f"After removing all-zero prices: {df.count()}")

# --- FILTER 5: Outlier detection ---
# A single-day high of $999,999 for a stock that normally trades at ~$380 is wrong.
# Simple approach: remove rows where high > 10x the close (unrealistic intraday range)
df = df.filter(F.col("high") <= F.col("close") * 10)
print(f"After removing outlier highs: {df.count()}")

# --- SUMMARY ---
count_after = df.count()
print(f"\n--- SUMMARY ---")
print(f"Started with: {count_before}")
print(f"Ended with:   {count_after}")
print(f"Removed:      {count_before - count_after} rows ({(count_before - count_after) / count_before * 100:.1f}%)")

print("\nClean data:")
df.show(truncate=False)

spark.stop()

# =============================================================================
# WHAT YOU LEARNED:
# - .filter() removes rows where the condition is False
# - ~ negates a condition (NOT)
# - & is AND, | is OR (must wrap each side in parentheses)
# - F.current_date() gives today's date for runtime comparisons
# - Always print counts before/after so you know what you removed
# - Filtering order doesn't matter for correctness, but logging each step
#   helps you debug which rule is too aggressive
# =============================================================================
