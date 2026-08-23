"""
Level 6: Derived Columns and Window Functions
==============================================
Goal: Compute new values from existing data — this is the "Transform" in ETL.

Key PySpark concepts:
- .withColumn() with expressions
- F.lag() / F.lead() — access previous/next row
- Window functions for running calculations
- F.when().when().otherwise() — multi-condition logic
- F.round(), F.abs(), F.lit()
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
spark = SparkSession.builder.appName("level-6").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.csv("data/dirty/market_data_dirty.csv", header=True, inferSchema=True)

# Full clean pipeline from levels 2-5
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

# --- DERIVED COLUMN 1: Daily return ---
# return = (today's close - yesterday's close) / yesterday's close
# Need lag() because "yesterday" depends on row order within each ticker
ticker_window = Window.partitionBy("ticker").orderBy("date")

df = df.withColumn(
    "prev_close", F.lag("close", 1).over(ticker_window)
)
df = df.withColumn(
    "daily_return",
    F.when(
        F.col("prev_close").isNotNull() & (F.col("prev_close") > 0),
        F.round((F.col("close") - F.col("prev_close")) / F.col("prev_close"), 6)
    )
)

# --- DERIVED COLUMN 2: Intraday range ---
# How much did the price move within the day? (high - low) / close
df = df.withColumn(
    "intraday_range_pct",
    F.round((F.col("high") - F.col("low")) / F.col("close"), 6)
)

# --- DERIVED COLUMN 3: Volume change ---
# Is today's volume higher or lower than yesterday's?
df = df.withColumn(
    "prev_volume", F.lag("volume", 1).over(ticker_window)
)
df = df.withColumn(
    "volume_change_pct",
    F.when(
        F.col("prev_volume").isNotNull() & (F.col("prev_volume") > 0),
        F.round((F.col("volume") - F.col("prev_volume")) / F.col("prev_volume"), 4)
    )
)

# --- DERIVED COLUMN 4: Price direction label ---
# Categorize the day: "up", "down", or "flat"
df = df.withColumn(
    "direction",
    F.when(F.col("daily_return") > 0.001, F.lit("up"))
     .when(F.col("daily_return") < -0.001, F.lit("down"))
     .otherwise(F.lit("flat"))
)

# --- DERIVED COLUMN 5: Running max (cumulative high) ---
# Useful for drawdown calculations later
running_window = Window.partitionBy("ticker").orderBy("date").rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)
df = df.withColumn(
    "cumulative_high", F.max("close").over(running_window)
)

# --- DERIVED COLUMN 6: Drawdown from peak ---
df = df.withColumn(
    "drawdown_from_peak",
    F.round((F.col("cumulative_high") - F.col("close")) / F.col("cumulative_high"), 6)
)

# --- SHOW RESULTS ---
print("Derived columns:")
df.select(
    "ticker", "date", "close", "daily_return", "intraday_range_pct",
    "direction", "cumulative_high", "drawdown_from_peak"
).orderBy("ticker", "date").show(30, truncate=False)

# Clean up helper columns before passing downstream
df_final = df.drop("prev_close", "prev_volume")
print("\nFinal schema:")
df_final.printSchema()

spark.stop()

# =============================================================================
# WHAT YOU LEARNED:
# - lag()/lead() access previous/next rows within a partition
# - Window.partitionBy("ticker").orderBy("date") = "within each stock, in date order"
# - rowsBetween(unboundedPreceding, currentRow) = running/cumulative calculation
# - F.when().when().otherwise() is PySpark's if/elif/else
# - Always guard division with isNotNull() and > 0 checks
# - Derived columns turn raw data into analytical features
# =============================================================================
