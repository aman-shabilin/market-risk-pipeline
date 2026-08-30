# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake Schema Setup
# MAGIC Creates the catalog, schema, and Delta tables for the Market Risk Pipeline.
# MAGIC
# MAGIC **Features demonstrated:**
# MAGIC - Unity Catalog (3-level namespace)
# MAGIC - Delta Lake table creation with explicit schema
# MAGIC - Table properties (delta.autoOptimize, liquid clustering)
# MAGIC - Constraints for data integrity

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "market_risk", "Catalog Name")
dbutils.widgets.text("schema", "analytics", "Schema Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
full_schema = f"{catalog}.{schema}"

print(f"Setting up: {full_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog and Schema

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema} COMMENT 'Market risk analytics pipeline'")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Market Prices Table (Bronze → Silver)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full_schema}.market_prices (
    ticker          STRING      NOT NULL    COMMENT 'Stock ticker symbol (uppercase)',
    date            DATE        NOT NULL    COMMENT 'Trading date',
    open            DOUBLE      NOT NULL    COMMENT 'Opening price',
    high            DOUBLE      NOT NULL    COMMENT 'Highest price of the day',
    low             DOUBLE      NOT NULL    COMMENT 'Lowest price of the day',
    close           DOUBLE      NOT NULL    COMMENT 'Closing price',
    volume          BIGINT      NOT NULL    COMMENT 'Trading volume',
    ingested_at     TIMESTAMP   NOT NULL    COMMENT 'Pipeline ingestion timestamp',
    source          STRING      NOT NULL    COMMENT 'Data source identifier (s3/yahoo/local)'
)
USING DELTA
COMMENT 'Daily OHLCV market data, deduplicated on (ticker, date)'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.columnMapping.mode' = 'name',
    'delta.enableChangeDataFeed' = 'true'
)
CLUSTER BY (ticker, date)
""")

# Add constraint for data integrity
spark.sql(f"""
ALTER TABLE {full_schema}.market_prices
ADD CONSTRAINT valid_price_range CHECK (high >= low AND low > 0)
""")

spark.sql(f"""
ALTER TABLE {full_schema}.market_prices
ADD CONSTRAINT valid_volume CHECK (volume >= 0)
""")

print("Created: market_prices")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Computed Metrics Table (Gold)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full_schema}.computed_metrics (
    ticker                  STRING      NOT NULL,
    window_start            DATE        NOT NULL,
    window_end              DATE        NOT NULL,
    annualized_volatility   DOUBLE      NOT NULL,
    var_95                  DOUBLE      NOT NULL,
    var_99                  DOUBLE      NOT NULL,
    cvar_95                 DOUBLE      NOT NULL,
    cvar_99                 DOUBLE      NOT NULL,
    sharpe_ratio            DOUBLE      NOT NULL,
    max_drawdown            DOUBLE      NOT NULL,
    computed_at             TIMESTAMP   NOT NULL,
    computation_duration_ms BIGINT      COMMENT 'Time taken to compute metrics'
)
USING DELTA
COMMENT 'Precomputed risk metrics per ticker and date window'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
)
CLUSTER BY (ticker, window_start)
""")

print("Created: computed_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Portfolio Tables

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full_schema}.portfolios (
    portfolio_id    STRING      NOT NULL    COMMENT 'Unique portfolio identifier',
    name            STRING      NOT NULL    COMMENT 'Human-readable portfolio name',
    created_at      TIMESTAMP   NOT NULL,
    updated_at      TIMESTAMP   NOT NULL
)
USING DELTA
COMMENT 'Portfolio definitions'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full_schema}.portfolio_holdings (
    portfolio_id    STRING      NOT NULL    COMMENT 'FK to portfolios',
    ticker          STRING      NOT NULL,
    weight          DOUBLE      NOT NULL    COMMENT 'Normalized weight (sums to 1.0)',
    updated_at      TIMESTAMP   NOT NULL
)
USING DELTA
COMMENT 'Portfolio composition with ticker weights'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
CLUSTER BY (portfolio_id)
""")

spark.sql(f"""
ALTER TABLE {full_schema}.portfolio_holdings
ADD CONSTRAINT valid_weight CHECK (weight > 0 AND weight <= 1)
""")

print("Created: portfolios, portfolio_holdings")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Monitoring Table

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full_schema}.pipeline_runs (
    run_id              STRING      NOT NULL,
    run_type            STRING      NOT NULL    COMMENT 'ingest / metrics / quality_check',
    status              STRING      NOT NULL    COMMENT 'running / succeeded / failed',
    started_at          TIMESTAMP   NOT NULL,
    finished_at         TIMESTAMP,
    rows_processed      BIGINT,
    rows_failed         BIGINT,
    tickers_processed   ARRAY<STRING>,
    error_message       STRING,
    parameters          STRING      COMMENT 'JSON of run parameters'
)
USING DELTA
COMMENT 'Pipeline execution audit log'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")

print("Created: pipeline_runs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Scores Table

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full_schema}.data_quality_scores (
    check_date          DATE        NOT NULL,
    ticker              STRING      NOT NULL,
    check_name          STRING      NOT NULL    COMMENT 'freshness / completeness / outliers / gaps',
    score               DOUBLE      NOT NULL    COMMENT '0.0 to 1.0 (1.0 = perfect)',
    details             STRING      COMMENT 'JSON with check-specific details',
    checked_at          TIMESTAMP   NOT NULL
)
USING DELTA
COMMENT 'Data quality scores per ticker per check type'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
CLUSTER BY (ticker, check_date)
""")

print("Created: data_quality_scores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ticker Universe Table
# MAGIC
# MAGIC The set of tickers the pipeline tracks. Kept in a table rather than a job
# MAGIC parameter because a 500-symbol comma-separated string in the workflow JSON is
# MAGIC neither reviewable nor queryable, and the sector column lets the dashboard roll
# MAGIC metrics up by sector. Populate it with `seed_ticker_universe`.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full_schema}.ticker_universe (
    ticker          STRING      NOT NULL    COMMENT 'Yahoo symbol; share classes use a dash',
    company_name    STRING                  COMMENT 'Registrant name',
    sector          STRING                  COMMENT 'GICS sector',
    active          BOOLEAN     NOT NULL    COMMENT 'False once a symbol leaves the index',
    added_at        TIMESTAMP   NOT NULL    COMMENT 'When the symbol first entered this table'
)
USING DELTA
COMMENT 'Tickers the pipeline ingests, with GICS sector for dashboard rollups'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.enableChangeDataFeed' = 'true'
)
""")

print("Created: ticker_universe")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Setup

# COMMAND ----------

tables = spark.sql(f"SHOW TABLES IN {full_schema}").collect()
print(f"\nTables in {full_schema}:")
for t in tables:
    print(f"  - {t.tableName}")
