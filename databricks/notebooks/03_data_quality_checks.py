# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Monitoring
# MAGIC
# MAGIC Runs systematic quality checks on market data and writes scores to the
# MAGIC `data_quality_scores` table for dashboard visualization.
# MAGIC
# MAGIC **Databricks features demonstrated:**
# MAGIC - Delta Lake time travel for freshness checks
# MAGIC - Window functions for gap detection
# MAGIC - Statistical outlier detection with Spark
# MAGIC - Quality scoring framework
# MAGIC - Pipeline monitoring patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "market_risk", "Catalog")
dbutils.widgets.text("schema", "analytics", "Schema")
dbutils.widgets.text("freshness_threshold_hours", "26", "Max hours since last ingest")
dbutils.widgets.text("completeness_lookback_days", "30", "Completeness check window")
dbutils.widgets.text("history_lookback_days", "180", "Outlier/gap check window")
dbutils.widgets.dropdown(
    "restrict_to_active_universe", "true", ["true", "false"], "Score only active tickers"
)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
freshness_threshold_hours = int(dbutils.widgets.get("freshness_threshold_hours"))
completeness_lookback_days = int(dbutils.widgets.get("completeness_lookback_days"))
history_lookback_days = int(dbutils.widgets.get("history_lookback_days"))
restrict_to_active_universe = dbutils.widgets.get("restrict_to_active_universe") == "true"

full_schema = f"{catalog}.{schema}"
prices_table = f"{full_schema}.market_prices"
quality_table = f"{full_schema}.data_quality_scores"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

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
    error_message = 'Run never reported completion; marked failed by a subsequent run. '
                 || 'Finish time unknown, so finished_at is left NULL.'
WHERE run_type = 'quality_check'
  AND status = 'running'
""")

run_params = json.dumps({
    "freshness_threshold_hours": freshness_threshold_hours,
    "completeness_lookback_days": completeness_lookback_days,
    "history_lookback_days": history_lookback_days,
    "restrict_to_active_universe": restrict_to_active_universe,
})

spark.sql(f"""
INSERT INTO {full_schema}.pipeline_runs
VALUES (
    '{run_id}', 'quality_check', 'running',
    current_timestamp(), NULL, 0, 0, NULL, NULL,
    '{run_params}'
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scope: Which Tickers Get Scored
# MAGIC
# MAGIC Every check below reads `scoped_prices` rather than `market_prices` directly.
# MAGIC A symbol that has left the index is stale by definition, so leaving it in the
# MAGIC scorecard means the freshness tile shows permanent failures nobody intends to fix
# MAGIC and the "tickers below threshold" trend never returns to zero. Its price history
# MAGIC stays in the table; only the scoring ignores it.

# COMMAND ----------

prices_all = spark.table(prices_table)
scope_note = "all tickers in market_prices"

if restrict_to_active_universe:
    universe_table = f"{full_schema}.ticker_universe"
    # Degrades to scoring everything rather than failing: an unseeded universe is a
    # setup step someone has not run yet, not a reason to lose today's scores.
    if not spark.catalog.tableExists(universe_table):
        print(f"WARNING: {universe_table} does not exist -- run setup_delta_tables.")
        active = spark.createDataFrame([], schema="ticker STRING")
    else:
        active = spark.sql(f"SELECT ticker FROM {universe_table} WHERE active")

    if active.isEmpty():
        print(
            "WARNING: no active tickers found -- scoring every ticker in market_prices. "
            "Run databricks/config/seed_ticker_universe to scope this."
        )
        prices_scoped = prices_all
    else:
        prices_scoped = prices_all.join(active, on="ticker", how="left_semi")
        scope_note = "active tickers in ticker_universe"
else:
    prices_scoped = prices_all

prices_scoped.createOrReplaceTempView("scoped_prices")
print(f"Scoring scope: {scope_note}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 1: Freshness
# MAGIC
# MAGIC How recently was each ticker updated? Score 1.0 if within threshold, decays to 0.

# COMMAND ----------

from pyspark.sql import functions as F

freshness_df = spark.sql("""
SELECT
    ticker,
    MAX(ingested_at) as last_ingested,
    MAX(date) as latest_date,
    TIMESTAMPDIFF(HOUR, MAX(ingested_at), current_timestamp()) as hours_since_ingest
FROM scoped_prices
GROUP BY ticker
""")

freshness_scores = (
    freshness_df
    .withColumn("score",
        F.when(F.col("hours_since_ingest") <= freshness_threshold_hours, 1.0)
        .when(F.col("hours_since_ingest") <= freshness_threshold_hours * 2, 0.7)
        .when(F.col("hours_since_ingest") <= freshness_threshold_hours * 4, 0.4)
        .otherwise(0.1)
    )
    .withColumn("check_name", F.lit("freshness"))
    .withColumn("check_date", F.current_date())
    .withColumn("checked_at", F.current_timestamp())
    .withColumn("details", F.to_json(F.struct(
        F.col("hours_since_ingest"),
        F.col("last_ingested"),
        F.col("latest_date")
    )))
    .select("check_date", "ticker", "check_name", "score", "details", "checked_at")
)

print("Freshness check:")
display(freshness_scores)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 2: Completeness
# MAGIC
# MAGIC Are there missing trading days? Compare actual dates to expected trading calendar.

# COMMAND ----------

from pyspark.sql.window import Window

completeness_df = spark.sql(f"""
WITH date_range AS (
    SELECT
        ticker,
        date,
        LAG(date) OVER (PARTITION BY ticker ORDER BY date) as prev_date,
        DATEDIFF(date, LAG(date) OVER (PARTITION BY ticker ORDER BY date)) as gap_days
    FROM scoped_prices
    WHERE date >= DATE_SUB(current_date(), {completeness_lookback_days})
),
ticker_stats AS (
    SELECT
        ticker,
        COUNT(*) as total_days,
        SUM(CASE WHEN gap_days > 3 THEN 1 ELSE 0 END) as gaps_detected,
        MAX(gap_days) as max_gap_days,
        COUNT(CASE WHEN gap_days > 3 THEN 1 END) as suspicious_gaps
    FROM date_range
    WHERE prev_date IS NOT NULL
    GROUP BY ticker
)
SELECT * FROM ticker_stats
""")

# Expected ~21 trading days per month
expected_days = completeness_lookback_days * 5 / 7  # rough weekday estimate

completeness_scores = (
    completeness_df
    .withColumn("coverage_ratio", F.col("total_days") / F.lit(expected_days))
    .withColumn("score",
        F.when(F.col("coverage_ratio") >= 0.95, 1.0)
        .when(F.col("coverage_ratio") >= 0.85, 0.8)
        .when(F.col("coverage_ratio") >= 0.70, 0.5)
        .otherwise(0.2)
    )
    .withColumn("check_name", F.lit("completeness"))
    .withColumn("check_date", F.current_date())
    .withColumn("checked_at", F.current_timestamp())
    .withColumn("details", F.to_json(F.struct(
        F.col("total_days"),
        F.col("gaps_detected"),
        F.col("max_gap_days"),
        F.round("coverage_ratio", 3).alias("coverage_ratio")
    )))
    .select("check_date", "ticker", "check_name", "score", "details", "checked_at")
)

print("Completeness check:")
display(completeness_scores)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 3: Outlier Detection
# MAGIC
# MAGIC Flag prices that are > 3 standard deviations from rolling mean.

# COMMAND ----------

rolling_window = Window.partitionBy("ticker").orderBy("date").rowsBetween(-20, -1)

# Bounded to a recent window, with extra days ahead of it to warm the 20-row rolling
# window up. Scored over all 20 years instead, the rate is dominated by 2008 and 2020:
# a symbol carries those outliers forever, and today's bad ingest moves a denominator of
# 5,000 points too little to change the score. Quality is a statement about now.
OUTLIER_WARMUP_DAYS = 45

outlier_source = spark.table("scoped_prices").filter(
    F.col("date") >= F.date_sub(F.current_date(), history_lookback_days + OUTLIER_WARMUP_DAYS)
)

outlier_df = (
    outlier_source
    .withColumn("rolling_mean", F.avg("close").over(rolling_window))
    .withColumn("rolling_std", F.stddev("close").over(rolling_window))
    .filter(F.col("rolling_std").isNotNull() & (F.col("rolling_std") > 0))
    .withColumn("z_score", F.abs(
        (F.col("close") - F.col("rolling_mean")) / F.col("rolling_std")
    ))
    .withColumn("is_outlier", F.col("z_score") > 3.0)
    .filter(F.col("date") >= F.date_sub(F.current_date(), history_lookback_days))
)

outlier_stats = (
    outlier_df
    .groupBy("ticker")
    .agg(
        F.count("*").alias("total_points"),
        F.sum(F.col("is_outlier").cast("int")).alias("outlier_count"),
        F.max("z_score").alias("max_z_score"),
    )
    .withColumn("outlier_rate", F.col("outlier_count") / F.col("total_points"))
)

outlier_scores = (
    outlier_stats
    .withColumn("score",
        F.when(F.col("outlier_rate") == 0, 1.0)
        .when(F.col("outlier_rate") < 0.01, 0.9)
        .when(F.col("outlier_rate") < 0.03, 0.6)
        .otherwise(0.3)
    )
    .withColumn("check_name", F.lit("outliers"))
    .withColumn("check_date", F.current_date())
    .withColumn("checked_at", F.current_timestamp())
    .withColumn("details", F.to_json(F.struct(
        F.col("outlier_count"),
        F.col("total_points"),
        F.round("outlier_rate", 4).alias("outlier_rate"),
        F.round("max_z_score", 2).alias("max_z_score")
    )))
    .select("check_date", "ticker", "check_name", "score", "details", "checked_at")
)

print("Outlier check:")
display(outlier_scores)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 4: Gap Detection (Trading Day Gaps)
# MAGIC
# MAGIC Specifically looks for gaps > 5 days that aren't holidays.

# COMMAND ----------

# Same window as the outlier check, and for the same reason: a >5-day gap happens
# legitimately around a few holidays, so over 20 years every symbol accumulates enough of
# them to score 0.2 and the check stops distinguishing anything. The widest gap in the
# window is kept because it is what an operator actually looks at.
gap_df = spark.sql(f"""
WITH gaps AS (
    SELECT
        ticker,
        date,
        LAG(date) OVER (PARTITION BY ticker ORDER BY date) as prev_date,
        DATEDIFF(date, LAG(date) OVER (PARTITION BY ticker ORDER BY date)) as gap_days
    FROM scoped_prices
    WHERE date >= DATE_SUB(current_date(), {history_lookback_days})
)
SELECT
    ticker,
    COUNT(CASE WHEN gap_days > 5 THEN 1 END) as large_gaps,
    COUNT(*) as total_transitions,
    MAX(gap_days) as widest_gap_days
FROM gaps
WHERE prev_date IS NOT NULL
GROUP BY ticker
""")

gap_scores = (
    gap_df
    .withColumn("score",
        F.when(F.col("large_gaps") == 0, 1.0)
        .when(F.col("large_gaps") <= 2, 0.8)
        .when(F.col("large_gaps") <= 5, 0.5)
        .otherwise(0.2)
    )
    .withColumn("check_name", F.lit("gaps"))
    .withColumn("check_date", F.current_date())
    .withColumn("checked_at", F.current_timestamp())
    .withColumn("details", F.to_json(F.struct(
        F.col("large_gaps"),
        F.col("total_transitions"),
        F.col("widest_gap_days")
    )))
    .select("check_date", "ticker", "check_name", "score", "details", "checked_at")
)

print("Gap detection check:")
display(gap_scores)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write All Scores to Quality Table

# COMMAND ----------

all_scores = (
    freshness_scores
    .union(completeness_scores)
    .union(outlier_scores)
    .union(gap_scores)
)

all_scores.createOrReplaceTempView("new_scores")

spark.sql(f"""
MERGE INTO {quality_table} AS target
USING new_scores AS source
ON target.ticker = source.ticker
   AND target.check_date = source.check_date
   AND target.check_name = source.check_name
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# Counted from the table, not from all_scores. `.cache()` is unavailable on serverless
# (NOT_SUPPORTED_WITH_SERVERLESS: PERSIST TABLE), so counting the DataFrame would re-run
# all four checks -- and the outlier check is a rolling window over every scoped ticker's
# recent history. The MERGE has already landed the rows; ask Delta instead.
total_checks = spark.sql(f"""
SELECT COUNT(*) AS scores
FROM {quality_table}
WHERE check_date = current_date()
""").collect()[0]["scores"]

print(f"\nWrote {total_checks} quality scores.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Summary (Overall Health Score)

# COMMAND ----------

summary = spark.sql(f"""
SELECT
    ticker,
    ROUND(AVG(score), 3) as overall_health_score,
    MIN(score) as worst_check_score,
    CONCAT_WS(', ',
        COLLECT_LIST(CASE WHEN score < 0.7 THEN check_name END)
    ) as failing_checks
FROM {quality_table}
WHERE check_date = current_date()
GROUP BY ticker
ORDER BY overall_health_score ASC
""")

display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Pipeline Run

# COMMAND ----------

spark.sql(f"""
UPDATE {full_schema}.pipeline_runs
SET
    status = 'succeeded',
    finished_at = current_timestamp(),
    rows_processed = {total_checks}
WHERE run_id = '{run_id}'
""")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "run_id": run_id,
    "status": "succeeded",
    "checks_completed": total_checks,
}))
