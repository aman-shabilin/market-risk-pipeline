"""
Level 3: Null Handling Strategies
=================================
Goal: Decide what to do with missing data — drop, fill, or flag.

Key PySpark concepts:
- F.col().isNull(), F.col().isNotNull()
- df.na.drop(subset=[...])
- df.na.fill({"column": value})
- F.coalesce() — first non-null value
- F.when().otherwise() — conditional logic
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.appName("level-3").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.csv("data/dirty/market_data_dirty.csv", header=True, inferSchema=True)

# Standardize first (from level 2)
df = (
    df
    .withColumn("ticker", F.upper(F.trim(F.col("ticker"))))
    .withColumn("date", F.col("date").cast("date"))
    .withColumn("open", F.expr("try_cast(open as DOUBLE)"))
    .withColumn("high", F.expr("try_cast(high as DOUBLE)"))
    .withColumn("low", F.expr("try_cast(low as DOUBLE)"))
    .withColumn("close", F.expr("try_cast(close as DOUBLE)"))
    .withColumn("volume", F.expr("try_cast(volume as INT)"))
)

print(f"Total rows after casting: {df.count()}")

# --- STRATEGY 1: Drop rows where critical columns are null ---
# ticker and date are identity — without them the row is useless
# close is needed for metrics — without it we can't compute returns
critical_cols = ["ticker", "date", "close"]
df_dropped = df.na.drop(subset=critical_cols)
print(f"\nAfter dropping rows with null ticker/date/close: {df_dropped.count()}")

# --- STRATEGY 2: Fill with a default ---
# volume=0 is a reasonable default (means "unknown volume", not "negative")
df_filled = df_dropped.na.fill({"volume": 0})

# --- STRATEGY 3: Fill open with close (if open is missing but close isn't) ---
# Reasoning: if we don't know the open, best guess is the close
df_filled = df_filled.withColumn(
    "open",
    F.coalesce(F.col("open"), F.col("close"))
)

# --- STRATEGY 4: Flag rows that had issues (for auditing) ---
# Add a column that says "this row had nulls we filled"
df_flagged = df_filled.withColumn(
    "was_imputed",
    F.when(
        F.col("open").isNull() | F.col("volume").isNull(),
        F.lit(True)
    ).otherwise(F.lit(False))
)

# Wait — the flag above won't work because we already filled the nulls!
# Correct approach: flag BEFORE filling, then fill
# This is a common mistake. Let's redo it properly:

df_with_flags = (
    df_dropped
    .withColumn("open_was_null", F.col("open").isNull())
    .withColumn("volume_was_null", F.col("volume").isNull())
    .withColumn("open", F.coalesce(F.col("open"), F.col("close")))
    .na.fill({"volume": 0})
)

print("\nRows with imputed values:")
df_with_flags.filter(
    F.col("open_was_null") | F.col("volume_was_null")
).show(truncate=False)

print("\nFinal null counts (should be zero for critical cols):")
df_with_flags.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in ["ticker", "date", "open", "high", "low", "close", "volume"]
]).show()

spark.stop()

# =============================================================================
# WHAT YOU LEARNED:
# - Drop when the row is useless without that value (identity columns)
# - Fill when you have a reasonable default
# - coalesce() picks the first non-null — great for fallback chains
# - Flag BEFORE you fill, not after (order matters!)
# - Different columns deserve different null strategies
# =============================================================================
