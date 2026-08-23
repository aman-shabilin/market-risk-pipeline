"""
Level 5: Deduplication Strategies
=================================
Goal: Handle duplicate rows — exact copies AND conflicting records for the same key.

Key PySpark concepts:
- .dropDuplicates() — remove exact copies
- .dropDuplicates(["col1", "col2"]) — dedup by key (keeps first)
- Window functions: F.row_number().over(Window.partitionBy(...).orderBy(...))
- Choosing WHICH duplicate to keep (latest, highest volume, etc.)
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
spark = SparkSession.builder.appName("level-5").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.csv("data/dirty/market_data_dirty.csv", header=True, inferSchema=True)

# Standardize (from previous levels)
# Use try_cast for all numeric columns — Spark 4.x ANSI mode rejects malformed strings with .cast()
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
    .na.fill({"volume": 0})
)

print(f"Rows before dedup: {df.count()}")

# --- STRATEGY 1: Drop exact duplicates ---
# Two rows that are 100% identical across ALL columns — safe to remove one.
df_exact = df.dropDuplicates()
print(f"After dropping exact duplicates: {df_exact.count()}")

# --- STRATEGY 2: Drop duplicates by key (naive) ---
# A stock can only have ONE price per day. (ticker, date) is the natural key.
# dropDuplicates(subset) keeps an arbitrary row — you don't control which one.
df_naive = df_exact.dropDuplicates(["ticker", "date"])
print(f"After naive key dedup: {df_naive.count()}")

# --- STRATEGY 3: Window function — keep the row with highest volume ---
# When two rows disagree for the same (ticker, date), we pick the one
# with more trading volume (more likely to be from a better source).
window = Window.partitionBy("ticker", "date").orderBy(F.col("volume").desc())

df_ranked = df_exact.withColumn("row_num", F.row_number().over(window))

# Show the conflicts before resolving
print("\nConflicting rows (same ticker+date, different values):")
conflicts = df_ranked.filter(F.col("row_num") > 1)
if conflicts.count() > 0:
    # Show both the winner (row_num=1) and losers for conflicting keys
    conflict_keys = conflicts.select("ticker", "date").distinct()
    df_ranked.join(conflict_keys, on=["ticker", "date"]).orderBy("ticker", "date", "row_num").show(
        truncate=False
    )

# Keep only the winners
df_deduped = df_ranked.filter(F.col("row_num") == 1).drop("row_num")
print(f"After window-based dedup (keep highest volume): {df_deduped.count()}")

# --- STRATEGY 4: For learning — see what was removed ---
print("\nFinal clean data:")
df_deduped.orderBy("ticker", "date").show(truncate=False)

spark.stop()

# =============================================================================
# WHAT YOU LEARNED:
# - dropDuplicates() = exact row match (safe, no data loss)
# - dropDuplicates(["key"]) = dedup by key, but non-deterministic (dangerous)
# - Window + row_number() = dedup by key WITH control over which row wins
# - The window pattern is: partition by the key, order by your preference,
#   keep row_number() == 1
# - This is THE most common dedup pattern in production pipelines
# =============================================================================
