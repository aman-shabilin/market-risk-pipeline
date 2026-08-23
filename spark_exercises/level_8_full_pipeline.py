"""
Level 8: Full Pipeline — Chain Everything Together
===================================================
Goal: One script that goes from raw dirty data to a clean, enriched, queryable table.
This is what a real pipeline job looks like.

Key PySpark concepts:
- Structuring a pipeline as functions
- .write (parquet, table, jdbc)
- spark.sql() — query DataFrames with SQL
- Quality metrics / audit log
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
spark = SparkSession.builder.appName("market-risk-pipeline").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


# ==========================================================================
# STEP 1: EXTRACT
# ==========================================================================
def extract(path: str):
    """Read raw data. Source doesn't matter — CSV, parquet, JDBC, Kafka."""
    return spark.read.csv(path, header=True, inferSchema=True)


# ==========================================================================
# STEP 2: STANDARDIZE
# ==========================================================================
def standardize(df):
    """Normalize strings and enforce types. No rows removed here."""
    return (
        df
        .withColumn("ticker", F.upper(F.trim(F.col("ticker"))))
        .withColumn("date", F.col("date").cast("date"))
        .withColumn("open", F.expr("try_cast(open as DOUBLE)"))
        .withColumn("high", F.expr("try_cast(high as DOUBLE)"))
        .withColumn("low", F.expr("try_cast(low as DOUBLE)"))
        .withColumn("close", F.expr("try_cast(close as DOUBLE)"))
        .withColumn("volume", F.expr("try_cast(volume as INT)"))
    )


# ==========================================================================
# STEP 3: CLEANSE
# ==========================================================================
def cleanse(df):
    """Remove invalid rows. Each filter has a business justification."""
    return (
        df
        # Identity columns must exist
        .na.drop(subset=["ticker", "date", "close"])
        # Fill recoverable nulls
        .withColumn("open", F.coalesce(F.col("open"), F.col("close")))
        .na.fill({"volume": 0})
        # Business rules
        .filter(F.col("volume") >= 0)           # no negative volume
        .filter(F.col("high") >= F.col("low"))  # valid candle
        .filter(F.col("date") <= F.current_date())  # no future dates
        .filter(~((F.col("open") == 0) & (F.col("close") == 0)))  # no zero rows
        .filter(F.col("high") <= F.col("close") * 10)  # no outliers
    )


# ==========================================================================
# STEP 4: DEDUPLICATE
# ==========================================================================
def deduplicate(df):
    """One row per (ticker, date). Highest volume wins conflicts."""
    window = Window.partitionBy("ticker", "date").orderBy(F.col("volume").desc())
    return (
        df
        .dropDuplicates()
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# ==========================================================================
# STEP 5: ENRICH
# ==========================================================================
def enrich(df):
    """Add derived columns that downstream analytics need."""
    ticker_window = Window.partitionBy("ticker").orderBy("date")
    running_window = Window.partitionBy("ticker").orderBy("date").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )

    return (
        df
        .withColumn("prev_close", F.lag("close", 1).over(ticker_window))
        .withColumn(
            "daily_return",
            F.when(
                F.col("prev_close").isNotNull() & (F.col("prev_close") > 0),
                F.round((F.col("close") - F.col("prev_close")) / F.col("prev_close"), 6)
            )
        )
        .withColumn(
            "intraday_range_pct",
            F.round((F.col("high") - F.col("low")) / F.col("close"), 6)
        )
        .withColumn("cumulative_high", F.max("close").over(running_window))
        .withColumn(
            "drawdown_from_peak",
            F.round(
                (F.col("cumulative_high") - F.col("close")) / F.col("cumulative_high"), 6
            )
        )
        .drop("prev_close")
    )


# ==========================================================================
# STEP 6: LOAD
# ==========================================================================
def load(df, output_path: str):
    """Write clean data. Parquet is the standard for analytics."""
    df.write.mode("overwrite").partitionBy("ticker").parquet(output_path)
    print(f"Written to: {output_path}")


# ==========================================================================
# STEP 7: QUALITY REPORT
# ==========================================================================
def quality_report(raw_count: int, clean_df):
    """Print what happened — every pipeline should report on data quality."""
    clean_count = clean_df.count()
    removed = raw_count - clean_count
    print("\n" + "=" * 50)
    print("DATA QUALITY REPORT")
    print("=" * 50)
    print(f"Raw rows:       {raw_count}")
    print(f"Clean rows:     {clean_count}")
    print(f"Removed:        {removed} ({removed / raw_count * 100:.1f}%)")
    print(f"Tickers:        {clean_df.select('ticker').distinct().count()}")
    print(f"Date range:     {clean_df.agg(F.min('date')).collect()[0][0]} → "
          f"{clean_df.agg(F.max('date')).collect()[0][0]}")
    print("=" * 50)

    # Per-ticker breakdown
    print("\nPer-ticker:")
    clean_df.groupBy("ticker").agg(
        F.count("*").alias("rows"),
        F.min("date").alias("first"),
        F.max("date").alias("last"),
        F.round(F.avg("daily_return"), 6).alias("avg_return"),
    ).show(truncate=False)


# ==========================================================================
# RUN THE PIPELINE
# ==========================================================================
if __name__ == "__main__":
    # Extract
    raw = extract("data/dirty/market_data_dirty.csv")
    raw_count = raw.count()

    # Transform
    clean = standardize(raw)
    clean = cleanse(clean)
    clean = deduplicate(clean)
    clean = enrich(clean)

    # Report
    quality_report(raw_count, clean)

    # Load
    load(clean, "output/market_prices_clean")

    # BONUS: Query with SQL
    clean.createOrReplaceTempView("prices")
    print("\n=== SQL Query: Best single-day returns ===")
    spark.sql("""
        SELECT ticker, date, close, daily_return
        FROM prices
        WHERE daily_return IS NOT NULL
        ORDER BY daily_return DESC
        LIMIT 5
    """).show(truncate=False)

    print("\n=== SQL Query: Average volume by ticker ===")
    spark.sql("""
        SELECT ticker,
               ROUND(AVG(volume), 0) as avg_volume,
               ROUND(AVG(daily_return), 6) as avg_daily_return
        FROM prices
        GROUP BY ticker
        ORDER BY avg_volume DESC
    """).show(truncate=False)

    spark.stop()

# =============================================================================
# WHAT YOU LEARNED:
# - A real pipeline is: extract → standardize → cleanse → dedup → enrich → load
# - Each step is a pure function: takes a DataFrame, returns a DataFrame
# - The quality report is NOT optional — you always need to know what you lost
# - Parquet (partitioned by ticker) is the standard output format
# - You can query the result with SQL using createOrReplaceTempView()
# - This entire script is what would run on a schedule (Airflow, cron, etc.)
# =============================================================================
