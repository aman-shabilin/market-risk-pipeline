"""
Level 1: Read and Inspect
=========================
Goal: Load data, look at it, understand what you're working with.

Key PySpark concepts:
- SparkSession.builder
- spark.read.csv()
- df.show(), df.printSchema(), df.count()
- df.describe()
- df.select(), df.columns
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("level-1").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# --- READ ---
# header=True: first row is column names
# inferSchema=True: Spark guesses types (we'll fix this manually later)
df = spark.read.csv("data/dirty/market_data_dirty.csv", header=True, inferSchema=True)

# --- INSPECT ---
# How many rows?
print(f"\nTotal rows: {df.count()}")

# What columns and types did Spark infer?
print("\nSchema:")
df.printSchema()

# Show first 20 rows (truncate=False shows full values)
print("\nFirst 20 rows:")
df.show(20, truncate=False)

# Basic stats — look for weird min/max values
print("\nDescribe (stats):")
df.describe().show()

# How many distinct tickers? (will reveal the case/whitespace issues)
print("\nDistinct tickers (raw):")
df.select("ticker").distinct().show(truncate=False)

# Count rows per ticker (will show duplicates and case issues)
print("\nRows per ticker:")
df.groupBy("ticker").count().show()

spark.stop()

# =============================================================================
# WHAT TO NOTICE:
# - ticker has "aapl", "AAPL", " AAPL " — same stock, different strings
# - volume has a null (Spark read "fifty_million" as null since inferSchema=True)
# - some rows have nulls in open, close
# - describe() shows min volume is -100 (invalid)
# - there's a row with date 2099 (future)
# - row count is higher than expected (duplicates)
# =============================================================================
