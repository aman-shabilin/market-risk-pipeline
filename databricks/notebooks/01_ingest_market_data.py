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
# MAGIC
# MAGIC **Running this notebook by hand:** add `yfinance` in the Environment side panel
# MAGIC first. The job supplies it through the `environments` block in
# MAGIC `databricks/workflows/market_risk_pipeline.json`, which an interactive serverless
# MAGIC session does not inherit — without it the `yahoo` source fails at `import
# MAGIC yfinance`. The other sources have no extra dependency.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "market_risk", "Catalog")
dbutils.widgets.text("schema", "analytics", "Schema")
dbutils.widgets.dropdown("source", "yahoo", ["yahoo", "s3", "volume", "auto_loader"], "Data Source")
dbutils.widgets.dropdown("ticker_source", "widget", ["widget", "table"], "Ticker List From")
dbutils.widgets.text("tickers", "AAPL,MSFT,GOOGL,AMZN,META", "Tickers (comma-separated)")
dbutils.widgets.text("lookback_days", "365", "Lookback Days")
dbutils.widgets.text("fetch_chunk_size", "25", "Symbols per Yahoo request")
dbutils.widgets.text("s3_path", "s3://market-data-bucket/ohlcv/", "S3/ADLS Path")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source = dbutils.widgets.get("source")
ticker_source = dbutils.widgets.get("ticker_source")
lookback_days = int(dbutils.widgets.get("lookback_days"))
fetch_chunk_size = int(dbutils.widgets.get("fetch_chunk_size"))
s3_path = dbutils.widgets.get("s3_path")

full_schema = f"{catalog}.{schema}"
target_table = f"{full_schema}.market_prices"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# The scheduled job reads the universe from ticker_universe: 500 symbols as a
# comma-separated job parameter is neither reviewable in a diff nor queryable, and
# every consumer would have to re-parse it. The widget path stays for ad-hoc runs.
if ticker_source == "table":
    universe_table = f"{full_schema}.ticker_universe"
    if not spark.catalog.tableExists(universe_table):
        raise ValueError(
            f"{universe_table} does not exist -- run databricks/config/setup_delta_tables, "
            "then databricks/config/seed_ticker_universe"
        )
    tickers = [
        row.ticker
        for row in spark.sql(
            f"SELECT ticker FROM {universe_table} WHERE active ORDER BY ticker"
        ).collect()
    ]
    if not tickers:
        raise ValueError(
            f"{universe_table} holds no active tickers -- "
            "run databricks/config/seed_ticker_universe first"
        )
else:
    tickers = [t.strip().upper() for t in dbutils.widgets.get("tickers").split(",") if t.strip()]

print(f"Source: {source}")
print(f"Tickers: {len(tickers)} from {ticker_source}")
print(f"  {tickers if len(tickers) <= 20 else tickers[:20] + ['...']}")
print(f"Lookback: {lookback_days} days")
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

# Reap orphaned runs before registering this one. A notebook that raises stops at the
# failing cell, so its final "succeeded" UPDATE never executes and the row is stranded
# at 'running' forever -- which silently skews the dashboard success-rate tile and keeps
# failed_runs at zero. The job sets max_concurrent_runs=1, so any 'ingest' row still
# 'running' as we start is by definition abandoned by an earlier run.
#
# finished_at is deliberately left NULL. Stamping current_timestamp() here dates the
# failure to whenever the *next* run happened to start, which for a row stranded a week
# ago reports a 6.8-day run and drags the dashboard's duration series with it. We do not
# know when the run died, and NULL says that; the duration tiles use
# TIMESTAMPDIFF(started_at, finished_at), which yields NULL and drops out of the chart.
spark.sql(f"""
UPDATE {full_schema}.pipeline_runs
SET status = 'failed',
    error_message = 'Run never reported completion; marked failed by a subsequent run. '
                 || 'Finish time unknown, so finished_at is left NULL.'
WHERE run_type = 'ingest'
  AND status = 'running'
""")

# Records the ticker count, not the list: with a 500-symbol universe the list would be
# a 4 KB string in every audit row, and tickers_processed already captures what landed.
run_params = json.dumps({
    "source": source,
    "ticker_source": ticker_source,
    "ticker_count": len(tickers),
    "lookback_days": lookback_days,
    "fetch_chunk_size": fetch_chunk_size,
})

spark.sql(f"""
INSERT INTO {full_schema}.pipeline_runs
VALUES (
    '{run_id}', 'ingest', 'running',
    current_timestamp(), NULL, 0, 0, NULL, NULL,
    '{run_params}'
)
""")

print(f"Pipeline run: {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source: Yahoo Finance

# COMMAND ----------

EMPTY_PRICE_SCHEMA = (
    "ticker STRING, date DATE, open DOUBLE, high DOUBLE, "
    "low DOUBLE, close DOUBLE, volume BIGINT"
)


def _tidy_ticker_frame(sub, ticker):
    """Reshape one ticker's OHLCV slice into the target column layout.

    Returns None when the slice holds no usable rows. A multi-symbol download pads
    unknown or delisted symbols with all-NaN columns rather than omitting them, so
    dropping incomplete rows here is what distinguishes "no data" from "bad data".
    """
    import pandas as pd

    df = sub.reset_index().rename(columns={
        "Date": "date", "Open": "open", "High": "high",
        "Low": "low", "Close": "close", "Volume": "volume",
    })
    df = df.dropna(subset=["open", "high", "low", "close", "volume"])
    if df.empty:
        return None

    df["ticker"] = ticker
    df["volume"] = df["volume"].astype("int64")
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df[["ticker", "date", "open", "high", "low", "close", "volume"]]


def ingest_from_yahoo(tickers, lookback_days, chunk_size=25, max_attempts=3):
    """Fetch OHLCV data from Yahoo Finance in chunks of symbols.

    `yf.download` accepts a list and returns one frame with a (field, ticker) column
    MultiIndex, so a single request covers many symbols. Fetching 500 symbols one at
    a time instead means 500 sequential round trips -- slow enough to dominate the
    run, and the pattern most likely to get throttled.

    A chunk that errors is retried with exponential backoff. One that keeps failing
    is reported and skipped rather than failing the run, because MERGE makes the next
    run pick up whatever was missed.

    Each chunk is handed to Spark as it arrives and the parts are unioned at the end.
    Concatenating every chunk into one pandas frame first would put the whole result in
    driver memory -- fine for 10 tickers over 30 days, but a 20-year backfill of the
    S&P 500 is ~2.5M rows, and the driver has no reason to hold them all at once.
    """
    import time
    from functools import reduce

    import pandas as pd
    import yfinance as yf
    from datetime import date, timedelta

    end = date.today()
    start = end - timedelta(days=lookback_days)

    parts = []
    rows_total = 0
    tickers_with_data = 0
    no_data = []
    failed_chunks = []

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        print(f"  Fetching {i + 1}-{i + len(chunk)} of {len(tickers)}...")

        raw = None
        for attempt in range(1, max_attempts + 1):
            try:
                raw = yf.download(
                    chunk,
                    start=start.isoformat(),
                    end=end.isoformat(),
                    progress=False,
                    auto_adjust=True,
                    group_by="column",
                    threads=True,
                )
                break
            except Exception as exc:  # noqa: BLE001 - yfinance raises assorted network errors
                if attempt == max_attempts:
                    print(f"  WARNING: chunk failed after {max_attempts} attempts: {exc}")
                    failed_chunks.extend(chunk)
                else:
                    time.sleep(2 ** attempt)

        if raw is None or raw.empty:
            no_data.extend(chunk)
            continue

        # Single-symbol responses come back flat; multi-symbol ones are keyed
        # (field, ticker) because of group_by="column".
        chunk_frames = []
        if isinstance(raw.columns, pd.MultiIndex):
            returned = set(raw.columns.get_level_values(-1))
            for ticker in chunk:
                if ticker not in returned:
                    no_data.append(ticker)
                    continue
                tidied = _tidy_ticker_frame(raw.xs(ticker, axis=1, level=-1), ticker)
                if tidied is None:
                    no_data.append(ticker)
                else:
                    chunk_frames.append(tidied)
        else:
            tidied = _tidy_ticker_frame(raw, chunk[0])
            if tidied is None:
                no_data.append(chunk[0])
            else:
                chunk_frames.append(tidied)

        if chunk_frames:
            combined = pd.concat(chunk_frames, ignore_index=True)
            rows_total += len(combined)
            tickers_with_data += len(chunk_frames)
            parts.append(spark.createDataFrame(combined))
            print(f"    {len(combined)} rows for {len(chunk_frames)} tickers")

    if no_data:
        print(f"  No data returned for {len(no_data)}: {sorted(no_data)}")
    if failed_chunks:
        print(f"  Fetch errors for {len(failed_chunks)}: {sorted(failed_chunks)}")

    if not parts:
        return spark.createDataFrame([], schema=EMPTY_PRICE_SCHEMA)

    print(f"  Fetched {rows_total} rows for {tickers_with_data} tickers")
    return reduce(lambda a, b: a.unionByName(b), parts)

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
    raw_df = ingest_from_yahoo(tickers, lookback_days, chunk_size=fetch_chunk_size)
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Validation & Cleansing

# COMMAND ----------

# The validity rule lives in one place and is used twice below -- once to filter and
# once to count what the filter dropped -- so the two cannot drift apart.
IS_VALID_ROW = (
    F.col("ticker").isNotNull()
    & (F.col("ticker") != "")
    & F.col("date").isNotNull()
    & (F.col("close") > 0)
    & (F.col("high") >= F.col("low"))
    & (F.col("volume") >= 0)
)

normalized_df = raw_df.withColumn("ticker", F.upper(F.trim(F.col("ticker"))))

validated_df = (
    normalized_df
    .filter(IS_VALID_ROW)
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("source", F.lit(source))
)

# Both counts come from one pass. `.cache()` is not available here -- serverless compute
# rejects it with NOT_SUPPORTED_WITH_SERVERLESS (PERSIST TABLE) -- so the way to avoid
# re-running the source is to ask fewer questions of it, rather than to materialise it.
# A separate `validated_df.count()` would re-serialise every fetched row a second time;
# summing the predicate as an integer answers both questions in a single scan.
counts = normalized_df.agg(
    F.count(F.lit(1)).alias("total_raw"),
    F.sum(IS_VALID_ROW.cast("int")).alias("total_valid"),
).collect()[0]

total_raw = counts["total_raw"]
total_valid = counts["total_valid"] or 0
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

# Read back from Delta rather than from validated_df: that DataFrame traces all the way
# to the fetched rows, so collecting from it would re-run the whole ingest a second time
# purely to list the symbols. The MERGE has already written them, `ingested_at` was
# stamped by this run, and both it and run_start come from the driver clock -- so
# ">= run_start" selects exactly this run's rows without depending on executor clocks.
tickers_processed = [
    row.ticker
    for row in (
        spark.table(target_table)
        .filter((F.col("source") == source) & (F.col("ingested_at") >= F.lit(run_start)))
        .select("ticker")
        .distinct()
        .collect()
    )
]

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
print(f"  Tickers: {len(tickers_processed)} of {len(tickers)} requested")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Ingestion

# COMMAND ----------

# Whole-table shape first, then the symbols that look wrong. Eyeballing a 500-row
# per-ticker listing after every run is not verification.
display(spark.sql(f"""
SELECT
    COUNT(*) as total_rows,
    COUNT(DISTINCT ticker) as tickers,
    MIN(date) as earliest_date,
    MAX(date) as latest_date,
    MAX(ingested_at) as last_ingested
FROM {target_table}
"""))

display(spark.sql(f"""
WITH latest AS (SELECT MAX(date) AS market_date FROM {target_table})
SELECT
    p.ticker,
    COUNT(*) as row_count,
    MIN(p.date) as earliest_date,
    MAX(p.date) as latest_date,
    MAX(p.ingested_at) as last_ingested
FROM {target_table} p
CROSS JOIN latest l
GROUP BY p.ticker, l.market_date
HAVING MAX(p.date) < DATE_SUB(l.market_date, 4)
ORDER BY latest_date, p.ticker
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output for Downstream Workflow Tasks

# COMMAND ----------

# Ticker count, not the list: the run-output panel in the Workflows UI truncates a
# 500-element array into uselessness, and pipeline_runs.tickers_processed has the detail.
dbutils.notebook.exit(json.dumps({
    "run_id": run_id,
    "status": "succeeded",
    "rows_processed": total_valid,
    "rows_failed": validation_failures,
    "tickers": len(tickers_processed),
}))
