"""
Level 2: Standardization
========================
Goal: Make data consistent before you filter or aggregate.

Key PySpark concepts:
- F.upper(), F.trim(), F.col()
- .withColumn() — add or replace a column
- .cast() — convert types
- .withColumnRenamed()
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.appName("level-2").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.csv("data/dirty/market_data_dirty.csv", header=True, inferSchema=True)

print("BEFORE standardization:")
df.select("ticker").distinct().show(truncate=False)

# --- STANDARDIZE TICKER ---
# Problem: "aapl", " AAPL ", "AAPL" should all be "AAPL"
# Fix: trim whitespace, then uppercase
df = df.withColumn("ticker", F.upper(F.trim(F.col("ticker"))))

print("AFTER standardization:")
df.select("ticker").distinct().show(truncate=False)

# --- CAST TYPES EXPLICITLY ---
# Problem: inferSchema guessed, but we want to be explicit
# Use try_cast so malformed values become null instead of throwing (Spark 4.x ANSI mode)
df = (
    df
    .withColumn("date", F.col("date").cast("date"))
    .withColumn("open", F.expr("try_cast(open as DOUBLE)"))
    .withColumn("high", F.expr("try_cast(high as DOUBLE)"))
    .withColumn("low", F.expr("try_cast(low as DOUBLE)"))
    .withColumn("close", F.expr("try_cast(close as DOUBLE)"))
    .withColumn("volume", F.expr("try_cast(volume as INT)"))
)

print("After explicit casting (bad values become null):")
df.printSchema()
df.show(truncate=False)

# --- COUNT WHAT BROKE ---
# How many rows have nulls now? (tells us how much data was unparseable)
print("Null counts per column:")
df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df.columns
]).show()

spark.stop()

# =============================================================================
# WHAT YOU LEARNED:
# - F.upper() + F.trim() is the standard "normalize string" pattern
# - .cast() is how you enforce types — bad values silently become null
# - Always cast explicitly rather than trusting inferSchema
# - After casting, count nulls to see how much damage the raw data had
# =============================================================================
