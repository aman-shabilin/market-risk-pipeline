# Databricks notebook source
# MAGIC %md
# MAGIC # Risk Metrics Computation
# MAGIC
# MAGIC Computes risk metrics (VaR, CVaR, Sharpe, Volatility, Drawdown) per ticker using
# MAGIC Spark + Pandas UDFs for scalable computation across hundreds of tickers.
# MAGIC
# MAGIC **Databricks features demonstrated:**
# MAGIC - Grouped-map Pandas function (`applyInPandas`) for complex per-group computation
# MAGIC - Window functions for rolling metrics
# MAGIC - Delta Lake MERGE for metrics upsert
# MAGIC - Parameterized notebooks (widgets)
# MAGIC - Performance monitoring (computation duration)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "market_risk", "Catalog")
dbutils.widgets.text("schema", "analytics", "Schema")
dbutils.widgets.text("risk_free_rate", "0.02", "Risk-Free Rate (annualized)")
dbutils.widgets.text("min_price_points", "30", "Minimum price points required")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
risk_free_rate = float(dbutils.widgets.get("risk_free_rate"))
min_price_points = int(dbutils.widgets.get("min_price_points"))

full_schema = f"{catalog}.{schema}"
prices_table = f"{full_schema}.market_prices"
metrics_table = f"{full_schema}.computed_metrics"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print(f"Risk-free rate: {risk_free_rate}")
print(f"Min price points: {min_price_points}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Run Tracking

# COMMAND ----------

import json
import uuid
from datetime import datetime, timezone

run_id = str(uuid.uuid4())

# Reap orphaned runs before registering this one -- see the note in 01_ingest_market_data.
spark.sql(f"""
UPDATE {full_schema}.pipeline_runs
SET status = 'failed',
    finished_at = COALESCE(finished_at, current_timestamp()),
    error_message = 'Run never reported completion; marked failed by a subsequent run'
WHERE run_type = 'metrics'
  AND status = 'running'
""")

spark.sql(f"""
INSERT INTO {full_schema}.pipeline_runs
VALUES (
    '{run_id}', 'metrics', 'running',
    current_timestamp(), NULL, 0, 0, NULL, NULL,
    '{json.dumps({"risk_free_rate": risk_free_rate, "min_price_points": min_price_points})}'
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Price Data

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

prices_df = spark.table(prices_table)

ticker_counts = (
    prices_df
    .groupBy("ticker")
    .agg(F.count("*").alias("num_days"))
    .filter(F.col("num_days") >= min_price_points)
)

eligible_tickers = [row.ticker for row in ticker_counts.select("ticker").collect()]
print(f"Eligible tickers (>= {min_price_points} days): {len(eligible_tickers)}")
print(f"  {eligible_tickers}")

# Filter to eligible tickers. No global sort here: the lag() window below orders within
# each ticker, and the grouped-map function sorts its own group -- an ordered scan would
# not survive the groupBy shuffle anyway.
prices_filtered = (
    prices_df
    .join(ticker_counts.select("ticker"), on="ticker")
    .select("ticker", "date", "close")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute Daily Returns (Window Function)

# COMMAND ----------

window_spec = Window.partitionBy("ticker").orderBy("date")

returns_df = (
    prices_filtered
    .withColumn("prev_close", F.lag("close", 1).over(window_spec))
    .filter(F.col("prev_close").isNotNull())
    .withColumn("daily_return", (F.col("close") - F.col("prev_close")) / F.col("prev_close"))
)

display(returns_df.filter(F.col("ticker") == "AAPL").orderBy("date").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute Risk Metrics with a Grouped-Map Pandas Function
# MAGIC
# MAGIC This is the core computation — a pandas function that receives all returns for one
# MAGIC ticker and outputs the full risk metric set. This scales to thousands of tickers
# MAGIC because Spark distributes groups across executors.
# MAGIC
# MAGIC Applied via `groupBy(...).applyInPandas(...)`, which replaces the deprecated
# MAGIC `pandas_udf(..., PandasUDFType.GROUPED_MAP)` API (removed in Spark 4).

# COMMAND ----------

import pandas as pd
import numpy as np
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, DoubleType, TimestampType, LongType
)

metrics_schema = StructType([
    StructField("ticker", StringType(), False),
    StructField("window_start", DateType(), False),
    StructField("window_end", DateType(), False),
    StructField("annualized_volatility", DoubleType(), False),
    StructField("var_95", DoubleType(), False),
    StructField("var_99", DoubleType(), False),
    StructField("cvar_95", DoubleType(), False),
    StructField("cvar_99", DoubleType(), False),
    StructField("sharpe_ratio", DoubleType(), False),
    StructField("max_drawdown", DoubleType(), False),
    StructField("computed_at", TimestampType(), False),
    StructField("computation_duration_ms", LongType(), False),
])

TRADING_DAYS = 252

def compute_metrics(pdf: pd.DataFrame) -> pd.DataFrame:
    """Compute full risk metric set for a single ticker.

    Receives every row of one ticker's group. Spark makes no ordering guarantee
    within a group, so the window bounds and the cumulative-return drawdown below
    would otherwise depend on shuffle order -- sort first.
    """
    import time

    start_time = time.time()

    pdf = pdf.sort_values("date")

    ticker = pdf["ticker"].iloc[0]
    returns = pdf["daily_return"].values
    dates = pdf["date"].values

    # Annualized volatility
    vol = float(np.std(returns, ddof=1) * np.sqrt(TRADING_DAYS))

    # Historical VaR
    var_95 = float(-np.percentile(returns, 5))
    var_99 = float(-np.percentile(returns, 1))

    # Conditional VaR (Expected Shortfall)
    tail_95 = returns[returns <= np.percentile(returns, 5)]
    cvar_95 = float(-tail_95.mean()) if len(tail_95) > 0 else var_95

    tail_99 = returns[returns <= np.percentile(returns, 1)]
    cvar_99 = float(-tail_99.mean()) if len(tail_99) > 0 else var_99

    # Sharpe ratio
    daily_rf = risk_free_rate / TRADING_DAYS
    excess = returns - daily_rf
    sharpe = float(excess.mean() / excess.std() * np.sqrt(TRADING_DAYS)) if excess.std() > 1e-10 else 0.0

    # Max drawdown (from cumulative returns)
    cumulative = np.cumprod(1 + returns)
    running_max = np.maximum.accumulate(cumulative)
    drawdowns = (cumulative - running_max) / running_max
    max_dd = float(-drawdowns.min()) if len(drawdowns) > 0 else 0.0

    duration_ms = int((time.time() - start_time) * 1000)

    return pd.DataFrame([{
        "ticker": ticker,
        "window_start": dates[0],
        "window_end": dates[-1],
        "annualized_volatility": vol,
        "var_95": var_95,
        "var_99": var_99,
        "cvar_95": cvar_95,
        "cvar_99": cvar_99,
        "sharpe_ratio": sharpe,
        "max_drawdown": max_dd,
        "computed_at": pd.Timestamp.now(tz="UTC"),
        "computation_duration_ms": duration_ms,
    }])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Computation

# COMMAND ----------

metrics_df = returns_df.groupBy("ticker").applyInPandas(compute_metrics, schema=metrics_schema)

# Drop tickers whose volatility came out NULL or NaN
metrics_clean = metrics_df.filter(
    F.col("annualized_volatility").isNotNull()
    & ~F.isnan(F.col("annualized_volatility"))
)

# Cached because the count, the display, the MERGE and the run-audit update below all
# consume this DataFrame; without it the whole grouped-map DAG re-runs each time and
# computed_at / computation_duration_ms differ between the merged rows and the display.
metrics_clean.cache()
metrics_count = metrics_clean.count()

print(f"Metrics computed for {metrics_count} tickers")
display(metrics_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE Metrics into Gold Table
# MAGIC
# MAGIC Upsert: if metrics for the same (ticker, window_start, window_end) exist, update them.

# COMMAND ----------

metrics_clean.createOrReplaceTempView("new_metrics")

spark.sql(f"""
MERGE INTO {metrics_table} AS target
USING new_metrics AS source
ON target.ticker = source.ticker
   AND target.window_start = source.window_start
   AND target.window_end = source.window_end
WHEN MATCHED THEN UPDATE SET
    target.annualized_volatility = source.annualized_volatility,
    target.var_95 = source.var_95,
    target.var_99 = source.var_99,
    target.cvar_95 = source.cvar_95,
    target.cvar_99 = source.cvar_99,
    target.sharpe_ratio = source.sharpe_ratio,
    target.max_drawdown = source.max_drawdown,
    target.computed_at = source.computed_at,
    target.computation_duration_ms = source.computation_duration_ms
WHEN NOT MATCHED THEN INSERT *
""")

print("Metrics merged into gold table.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rolling Metrics (Window Functions)
# MAGIC
# MAGIC Compute 21-day rolling volatility and mean return for time-series visualization.
# MAGIC
# MAGIC The view below is the **single definition** of the rolling window. An equivalent
# MAGIC PySpark version used to sit alongside it, unused, which left two implementations
# MAGIC free to drift apart -- the same failure `metrics/service.py` prevents on the
# MAGIC standalone side. Rows inside the 21-day warmup are excluded, since a 21-day
# MAGIC volatility computed from two observations is not a 21-day volatility.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {full_schema}.v_rolling_metrics AS
WITH daily AS (
    SELECT
        ticker,
        date,
        close,
        close / LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date) - 1 AS daily_return
    FROM {full_schema}.market_prices
    WHERE close > 0
),
rolling AS (
    SELECT
        ticker,
        date,
        close,
        daily_return,
        STDDEV(daily_return)
            OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 20 PRECEDING AND CURRENT ROW)
            * SQRT({TRADING_DAYS}) AS rolling_vol_21d,
        AVG(daily_return)
            OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 20 PRECEDING AND CURRENT ROW)
            AS rolling_mean_21d,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) AS row_num
    FROM daily
)
SELECT ticker, date, close, daily_return, rolling_vol_21d, rolling_mean_21d
FROM rolling
WHERE row_num > 21
""")

print("Rolling metrics view created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Pipeline Run

# COMMAND ----------

tickers_computed = [row.ticker for row in metrics_clean.select("ticker").distinct().collect()]

spark.sql(f"""
UPDATE {full_schema}.pipeline_runs
SET
    status = 'succeeded',
    finished_at = current_timestamp(),
    rows_processed = {metrics_count},
    tickers_processed = array({', '.join([f"'{t}'" for t in tickers_computed])})
WHERE run_id = '{run_id}'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

display(spark.sql(f"""
SELECT
    ticker,
    window_start,
    window_end,
    ROUND(annualized_volatility, 4) as volatility,
    ROUND(var_95, 4) as var_95,
    ROUND(cvar_95, 4) as cvar_95,
    ROUND(sharpe_ratio, 4) as sharpe,
    ROUND(max_drawdown, 4) as max_drawdown,
    computation_duration_ms
FROM {metrics_table}
ORDER BY ticker
"""))

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "run_id": run_id,
    "status": "succeeded",
    "tickers_computed": tickers_computed,
}))
