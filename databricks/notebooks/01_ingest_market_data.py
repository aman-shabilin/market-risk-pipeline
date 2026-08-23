# Databricks notebook source
# MAGIC %md
# MAGIC # Market Data Ingestion Pipeline
# MAGIC
# MAGIC Ingests OHLCV data from multiple sources into Delta Lake using **MERGE** for idempotency.
# MAGIC
# MAGIC **Databricks features demonstrated:**
# MAGIC - Widget-driven parameterization
# MAGIC - Multi-source ingestion (S3/ADLS, Yahoo Finance, Volumes)
# MAGIC - Delta Lake MERGE (upsert) for exactly-once semantics
# MAGIC - Schema enforcement and evolution
# MAGIC - Change Data Feed for downstream consumers
# MAGIC - Audit trail via pipeline_runs table
# MAGIC - Structured Streaming readiness (Auto Loader pattern)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "market_risk", "Catalog")
dbutils.widgets.text("schema", "analytics", "Schema")
dbutils.widgets.dropdown("source", "yahoo", ["yahoo", "s3", "volume", "auto_loader"], "Data Source")
dbutils.widgets.text("tickers", "AAPL,MSFT,GOOGL,AMZN,META", "Tickers (comma-separated)")
dbutils.widgets.text("lookback_days", "365", "Lookback Days")
dbutils.widgets.text("s3_path", "s3://market-data-bucket/ohlcv/", "S3/ADLS Path")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source = dbutils.widgets.get("source")
tickers = [t.strip().upper() for t in dbutils.widgets.get("tickers").split(",")]
lookback_days = int(dbutils.widgets.get("lookback_days"))
s3_path = dbutils.widgets.get("s3_path")

full_schema = f"{catalog}.{schema}"
target_table = f"{full_schema}.market_prices"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print(f"Source: {source}")
print(f"Tickers: {tickers}")
print(f"Target: {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Run Tracking

# COMMAND ----------

import json
import uuid
from datetime import datetime, timezone

run_id = str(uuid.uuid4())
run_start = datetime.now(timezone.utc)

spark.sql(f"""
INSERT INTO {full_schema}.pipeline_runs
VALUES (
    '{run_id}', 'ingest', 'running',
    current_timestamp(), NULL, 0, 0, NULL, NULL,
    '{json.dumps({"source": source, "tickers": tickers, "lookback_days": lookback_days})}'
)
""")

print(f"Pipeline run: {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source: Yahoo Finance

# COMMAND ----------

def ingest_from_yahoo(tickers, lookback_days):
    """Fetch OHLCV data from Yahoo Finance using yfinance."""
    import yfinance as yf
    import pandas as pd
    from datetime import date, timedelta

    end = date.today()
    start = end - timedelta(days=lookback_days)

    all_data = []
    for ticker in tickers:
        print(f"  Fetching {ticker}...")
        raw = yf.download(
            ticker,
            start=start.isoformat(),
            end=end.isoformat(),
            progress=False,
            auto_adjust=True,
        )
        if raw.empty:
            print(f"  WARNING: No data for {ticker}")
            continue

        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)

        df = raw.reset_index()
        df = df.rename(columns={
            "Date": "date", "Open": "open", "High": "high",
            "Low": "low", "Close": "close", "Volume": "volume",
        })
        df["ticker"] = ticker
        df["volume"] = df["volume"].astype("int64")
        df["date"] = pd.to_datetime(df["date"]).dt.date
        all_data.append(df[["ticker", "date", "open", "high", "low", "close", "volume"]])

    if not all_data:
        return spark.createDataFrame([], schema="ticker STRING, date DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume BIGINT")

    combined = pd.concat(all_data, ignore_index=True)
    return spark.createDataFrame(combined)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source: S3 / ADLS (Batch)

# COMMAND ----------

def ingest_from_cloud_storage(path):
    """Read CSV files from S3 or ADLS with schema enforcement."""
    from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, LongType

    expected_schema = StructType([
        StructField("ticker", StringType(), False),
        StructField("date", DateType(), False),
        StructField("open", DoubleType(), False),
        StructField("high", DoubleType(), False),
        StructField("low", DoubleType(), False),
        StructField("close", DoubleType(), False),
        StructField("volume", LongType(), False),
    ])

    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("dateFormat", "yyyy-MM-dd")
        .schema(expected_schema)
        .load(path)
    )
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source: Auto Loader (Incremental / Streaming)

# COMMAND ----------

def ingest_with_auto_loader(path, checkpoint_path):
    """
    Demonstrates Auto Loader for incremental file ingestion.
    Processes only new files since last run.
    """
    from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, LongType

    schema = StructType([
        StructField("ticker", StringType(), False),
        StructField("date", DateType(), False),
        StructField("open", DoubleType(), False),
        StructField("high", DoubleType(), False),
        StructField("low", DoubleType(), False),
        StructField("close", DoubleType(), False),
        StructField("volume", LongType(), False),
    ])

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", checkpoint_path)
        .option("header", "true")
        .schema(schema)
        .load(path)
    )
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source: Databricks Volume (Unity Catalog Managed)

# COMMAND ----------

def ingest_from_volume(catalog, schema):
    """Read from Unity Catalog managed volume."""
    volume_path = f"/Volumes/{catalog}/{schema}/market_data/"

    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(volume_path)
    )
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Ingestion

# COMMAND ----------

from pyspark.sql import functions as F

if source == "yahoo":
    raw_df = ingest_from_yahoo(tickers, lookback_days)
elif source == "s3":
    raw_df = ingest_from_cloud_storage(s3_path)
elif source == "volume":
    raw_df = ingest_from_volume(catalog, schema)
elif source == "auto_loader":
    checkpoint = f"/Volumes/{catalog}/{schema}/checkpoints/market_data"
    raw_df = ingest_with_auto_loader(s3_path, checkpoint)
    # Auto Loader uses streaming — handle separately
    print("Auto Loader streaming mode — see streaming section")
else:
    raise ValueError(f"Unknown source: {source}")

print(f"Raw records fetched: {raw_df.count() if source != 'auto_loader' else 'streaming'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Validation & Cleansing

# COMMAND ----------

validated_df = (
    raw_df
    .withColumn("ticker", F.upper(F.trim(F.col("ticker"))))
    .filter(F.col("ticker").isNotNull() & (F.col("ticker") != ""))
    .filter(F.col("date").isNotNull())
    .filter(F.col("close") > 0)
    .filter(F.col("high") >= F.col("low"))
    .filter(F.col("volume") >= 0)
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("source", F.lit(source))
)

# Count validation failures
total_raw = raw_df.count()
total_valid = validated_df.count()
validation_failures = total_raw - total_valid

print(f"Total raw rows:        {total_raw}")
print(f"Valid rows:            {total_valid}")
print(f"Validation failures:   {validation_failures}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake MERGE (Upsert)
# MAGIC
# MAGIC Uses `MERGE INTO` for idempotent ingestion:
# MAGIC - Match on `(ticker, date)` — natural key
# MAGIC - If matched: update prices (data correction)
# MAGIC - If not matched: insert new row

# COMMAND ----------

validated_df.createOrReplaceTempView("incoming_prices")

merge_result = spark.sql(f"""
MERGE INTO {target_table} AS target
USING incoming_prices AS source
ON target.ticker = source.ticker AND target.date = source.date
WHEN MATCHED THEN UPDATE SET
    target.open = source.open,
    target.high = source.high,
    target.low = source.low,
    target.close = source.close,
    target.volume = source.volume,
    target.ingested_at = source.ingested_at,
    target.source = source.source
WHEN NOT MATCHED THEN INSERT *
""")

# Extract merge metrics
merge_metrics = spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1").collect()[0]
print(f"\nMerge complete.")
print(f"  Operation: {merge_metrics['operation']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Pipeline Run Status

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp, array

tickers_processed = [row.ticker for row in validated_df.select("ticker").distinct().collect()]

spark.sql(f"""
UPDATE {full_schema}.pipeline_runs
SET
    status = 'succeeded',
    finished_at = current_timestamp(),
    rows_processed = {total_valid},
    rows_failed = {validation_failures},
    tickers_processed = array({', '.join([f"'{t}'" for t in tickers_processed])})
WHERE run_id = '{run_id}'
""")

print(f"\nPipeline run {run_id} completed successfully.")
print(f"  Tickers: {tickers_processed}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Ingestion

# COMMAND ----------

summary = spark.sql(f"""
SELECT
    ticker,
    COUNT(*) as row_count,
    MIN(date) as earliest_date,
    MAX(date) as latest_date,
    MAX(ingested_at) as last_ingested
FROM {target_table}
GROUP BY ticker
ORDER BY ticker
""")

display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output for Downstream Workflow Tasks

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "run_id": run_id,
    "status": "succeeded",
    "rows_processed": total_valid,
    "rows_failed": validation_failures,
    "tickers": tickers_processed,
}))
