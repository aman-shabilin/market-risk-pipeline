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

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
freshness_threshold_hours = int(dbutils.widgets.get("freshness_threshold_hours"))
completeness_lookback_days = int(dbutils.widgets.get("completeness_lookback_days"))

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
    finished_at = COALESCE(finished_at, current_timestamp()),
    error_message = 'Run never reported completion; marked failed by a subsequent run'
WHERE run_type = 'quality_check'
  AND status = 'running'
""")

spark.sql(f"""
INSERT INTO {full_schema}.pipeline_runs
VALUES (
    '{run_id}', 'quality_check', 'running',
    current_timestamp(), NULL, 0, 0, NULL, NULL,
    '{json.dumps({"freshness_threshold_hours": freshness_threshold_hours})}'
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 1: Freshness
# MAGIC
# MAGIC How recently was each ticker updated? Score 1.0 if within threshold, decays to 0.

# COMMAND ----------

from pyspark.sql import functions as F

freshness_df = spark.sql(f"""
SELECT
    ticker,
    MAX(ingested_at) as last_ingested,
    MAX(date) as latest_date,
    TIMESTAMPDIFF(HOUR, MAX(ingested_at), current_timestamp()) as hours_since_ingest
FROM {prices_table}
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
    FROM {prices_table}
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

outlier_df = (
    spark.table(prices_table)
    .withColumn("rolling_mean", F.avg("close").over(rolling_window))
    .withColumn("rolling_std", F.stddev("close").over(rolling_window))
    .filter(F.col("rolling_std").isNotNull() & (F.col("rolling_std") > 0))
    .withColumn("z_score", F.abs(
        (F.col("close") - F.col("rolling_mean")) / F.col("rolling_std")
    ))
    .withColumn("is_outlier", F.col("z_score") > 3.0)
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

gap_df = spark.sql(f"""
WITH gaps AS (
    SELECT
        ticker,
        date,
        LAG(date) OVER (PARTITION BY ticker ORDER BY date) as prev_date,
        DATEDIFF(date, LAG(date) OVER (PARTITION BY ticker ORDER BY date)) as gap_days
    FROM {prices_table}
)
SELECT
    ticker,
    COUNT(CASE WHEN gap_days > 5 THEN 1 END) as large_gaps,
    COUNT(*) as total_transitions,
    COLLECT_LIST(
        CASE WHEN gap_days > 5
        THEN CONCAT(CAST(prev_date AS STRING), ' to ', CAST(date AS STRING), ' (', CAST(gap_days AS STRING), 'd)')
        END
    ) as gap_details
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
        F.col("total_transitions")
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

total_checks = all_scores.count()
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
