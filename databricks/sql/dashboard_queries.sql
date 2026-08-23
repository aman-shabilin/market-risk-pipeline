-- ============================================================================
-- DATABRICKS SQL DASHBOARD QUERIES
-- Market Risk Pipeline - Operations & Analytics Dashboard
-- ============================================================================
-- Import these queries into a Databricks SQL Dashboard.
-- Each section becomes a dashboard widget/tile.
-- ============================================================================


-- ============================================================================
-- SECTION 1: PIPELINE HEALTH (Operations View)
-- ============================================================================

-- WIDGET: Pipeline Run History (Time Series)
-- Type: Line chart (x=started_at, y=rows_processed, color=status)
SELECT
    DATE(started_at) as run_date,
    run_type,
    status,
    rows_processed,
    rows_failed,
    TIMESTAMPDIFF(SECOND, started_at, finished_at) as duration_seconds
FROM market_risk.analytics.pipeline_runs
WHERE started_at >= DATE_SUB(current_date(), 30)
ORDER BY started_at DESC;


-- WIDGET: Pipeline Success Rate (Counter/Stat Tile)
-- Type: Counter showing percentage
SELECT
    ROUND(
        COUNT(CASE WHEN status = 'succeeded' THEN 1 END) * 100.0 / COUNT(*), 1
    ) as success_rate_pct,
    COUNT(*) as total_runs,
    COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_runs
FROM market_risk.analytics.pipeline_runs
WHERE started_at >= DATE_SUB(current_date(), 7);


-- WIDGET: Last Run Status (Table)
-- Type: Table with conditional formatting
SELECT
    run_type,
    status,
    started_at,
    finished_at,
    rows_processed,
    rows_failed,
    TIMESTAMPDIFF(SECOND, started_at, finished_at) as duration_sec,
    tickers_processed
FROM market_risk.analytics.pipeline_runs
WHERE started_at >= DATE_SUB(current_date(), 1)
ORDER BY started_at DESC;


-- ============================================================================
-- SECTION 2: DATA QUALITY SCORECARD
-- ============================================================================

-- WIDGET: Overall Quality Score by Ticker (Heatmap)
-- Type: Heatmap (x=check_name, y=ticker, value=score)
SELECT
    ticker,
    check_name,
    score,
    details
FROM market_risk.analytics.data_quality_scores
WHERE check_date = current_date()
ORDER BY ticker, check_name;


-- WIDGET: Quality Trend Over Time (Line Chart)
-- Type: Line chart (x=check_date, y=avg_score, color=check_name)
SELECT
    check_date,
    check_name,
    ROUND(AVG(score), 3) as avg_score,
    MIN(score) as min_score,
    COUNT(CASE WHEN score < 0.7 THEN 1 END) as tickers_below_threshold
FROM market_risk.analytics.data_quality_scores
WHERE check_date >= DATE_SUB(current_date(), 30)
GROUP BY check_date, check_name
ORDER BY check_date, check_name;


-- WIDGET: Tickers Needing Attention (Table with alerts)
-- Type: Table, filtered to failing checks
SELECT
    ticker,
    check_name,
    score,
    details,
    checked_at
FROM market_risk.analytics.data_quality_scores
WHERE check_date = current_date()
  AND score < 0.7
ORDER BY score ASC;


-- ============================================================================
-- SECTION 3: RISK METRICS OVERVIEW
-- ============================================================================

-- WIDGET: Current Risk Metrics (Table)
-- Type: Table with sparklines or bars
SELECT
    ticker,
    window_start,
    window_end,
    ROUND(annualized_volatility * 100, 2) as volatility_pct,
    ROUND(var_95 * 100, 2) as var_95_pct,
    ROUND(cvar_95 * 100, 2) as cvar_95_pct,
    ROUND(sharpe_ratio, 3) as sharpe_ratio,
    ROUND(max_drawdown * 100, 2) as max_drawdown_pct,
    computed_at
FROM market_risk.analytics.computed_metrics
ORDER BY ticker;


-- WIDGET: Volatility Comparison (Bar Chart)
-- Type: Bar chart (x=ticker, y=volatility_pct)
SELECT
    ticker,
    ROUND(annualized_volatility * 100, 2) as volatility_pct,
    ROUND(var_95 * 100, 2) as var_95_pct,
    ROUND(max_drawdown * 100, 2) as max_drawdown_pct
FROM market_risk.analytics.computed_metrics
ORDER BY annualized_volatility DESC;


-- WIDGET: Risk-Return Scatter (Scatter Plot)
-- Type: Scatter (x=volatility, y=sharpe_ratio, label=ticker)
SELECT
    ticker,
    ROUND(annualized_volatility, 4) as volatility,
    ROUND(sharpe_ratio, 4) as sharpe_ratio,
    ROUND(var_95, 4) as var_95
FROM market_risk.analytics.computed_metrics;


-- WIDGET: VaR vs CVaR Comparison (Grouped Bar)
-- Type: Grouped bar chart showing VaR and CVaR side by side
SELECT
    ticker,
    ROUND(var_95 * 100, 3) as var_95_pct,
    ROUND(cvar_95 * 100, 3) as cvar_95_pct,
    ROUND(var_99 * 100, 3) as var_99_pct,
    ROUND(cvar_99 * 100, 3) as cvar_99_pct
FROM market_risk.analytics.computed_metrics
ORDER BY var_95 DESC;


-- ============================================================================
-- SECTION 4: ROLLING METRICS (Time Series View)
-- ============================================================================

-- WIDGET: Rolling Volatility Over Time (Line Chart)
-- Type: Multi-line chart (x=date, y=rolling_vol_21d, color=ticker)
SELECT
    ticker,
    date,
    ROUND(rolling_vol_21d * 100, 2) as rolling_vol_pct,
    ROUND(daily_return * 100, 3) as daily_return_pct
FROM market_risk.analytics.v_rolling_metrics
WHERE date >= DATE_SUB(current_date(), 90)
ORDER BY ticker, date;


-- WIDGET: Return Distribution (Histogram data)
-- Type: Histogram or box plot per ticker
SELECT
    ticker,
    ROUND(daily_return * 100, 3) as return_pct,
    date
FROM market_risk.analytics.v_rolling_metrics
WHERE date >= DATE_SUB(current_date(), 252)
ORDER BY ticker, date;


-- ============================================================================
-- SECTION 5: DATA COVERAGE & INVENTORY
-- ============================================================================

-- WIDGET: Data Inventory (Table)
-- Type: Table showing what data we have
SELECT
    ticker,
    COUNT(*) as total_records,
    MIN(date) as first_date,
    MAX(date) as last_date,
    DATEDIFF(MAX(date), MIN(date)) as date_span_days,
    MAX(ingested_at) as last_ingested,
    TIMESTAMPDIFF(HOUR, MAX(ingested_at), current_timestamp()) as hours_since_update
FROM market_risk.analytics.market_prices
GROUP BY ticker
ORDER BY ticker;


-- WIDGET: Daily Ingestion Volume (Area Chart)
-- Type: Stacked area chart (x=date, y=records, color=ticker)
SELECT
    DATE(ingested_at) as ingest_date,
    ticker,
    COUNT(*) as records_ingested
FROM market_risk.analytics.market_prices
WHERE ingested_at >= DATE_SUB(current_date(), 30)
GROUP BY DATE(ingested_at), ticker
ORDER BY ingest_date, ticker;


-- WIDGET: Source Distribution (Pie Chart)
-- Type: Pie/donut chart
SELECT
    source,
    COUNT(*) as record_count,
    COUNT(DISTINCT ticker) as ticker_count
FROM market_risk.analytics.market_prices
GROUP BY source;


-- ============================================================================
-- SECTION 6: DELTA LAKE OPERATIONS (Advanced)
-- ============================================================================

-- WIDGET: Table Size and History
-- Shows Delta Lake operational metadata
DESCRIBE HISTORY market_risk.analytics.market_prices LIMIT 10;


-- WIDGET: Table Detail
DESCRIBE DETAIL market_risk.analytics.market_prices;


-- WIDGET: Change Data Feed (Recent Changes)
-- Useful for auditing what data changed
SELECT *
FROM table_changes('market_risk.analytics.market_prices', 1)
ORDER BY _commit_timestamp DESC
LIMIT 100;
