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
-- tickers_processed holds ~500 symbols, so the count goes in the table and the array
-- stays out of it; the coverage-gaps tile in section 5 is where missing symbols surface.
SELECT
    run_type,
    status,
    started_at,
    finished_at,
    rows_processed,
    rows_failed,
    TIMESTAMPDIFF(SECOND, started_at, finished_at) as duration_sec,
    SIZE(tickers_processed) as tickers_processed_count,
    error_message
FROM market_risk.analytics.pipeline_runs
WHERE started_at >= DATE_SUB(current_date(), 1)
ORDER BY started_at DESC;


-- ============================================================================
-- SECTION 2: DATA QUALITY SCORECARD
-- ============================================================================

-- WIDGET: Overall Quality Score by Ticker (Heatmap)
-- Type: Heatmap (x=check_name, y=ticker, value=score)
-- Restricted to the 40 weakest tickers: a heatmap of the whole 500-symbol universe is
-- 2,000 cells, which reads as wallpaper. The rows worth looking at are the bad ones.
WITH today AS (
    SELECT ticker, check_name, score, details
    FROM market_risk.analytics.data_quality_scores
    WHERE check_date = current_date()
),
worst AS (
    SELECT ticker
    FROM today
    GROUP BY ticker
    ORDER BY MIN(score) ASC, AVG(score) ASC
    LIMIT 40
)
SELECT t.ticker, t.check_name, t.score, t.details
FROM today t
JOIN worst w ON t.ticker = w.ticker
ORDER BY t.ticker, t.check_name;


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
-- Worst 100 first: across 500 tickers and 4 checks a bad ingest can fail thousands of
-- rows, and nobody works a list that long -- they work the top of it.
SELECT
    ticker,
    check_name,
    score,
    details,
    checked_at
FROM market_risk.analytics.data_quality_scores
WHERE check_date = current_date()
  AND score < 0.7
ORDER BY score ASC
LIMIT 100;


-- ============================================================================
-- SECTION 3: RISK METRICS OVERVIEW
-- ============================================================================

-- Every tile in this section reduces to the latest snapshot per ticker with QUALIFY.
-- computed_metrics is keyed (ticker, window_start, window_end), so a daily run over a
-- growing history inserts a new row per ticker per day rather than replacing one --
-- without the filter each tile would plot every historical snapshot at once.

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
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ticker ORDER BY window_end DESC, computed_at DESC
) = 1
ORDER BY ticker;


-- WIDGET: Volatility Comparison (Bar Chart)
-- Type: Bar chart (x=ticker, y=volatility_pct)
-- Top 25 only: 500 bars is not a chart.
SELECT
    ticker,
    ROUND(annualized_volatility * 100, 2) as volatility_pct,
    ROUND(var_95 * 100, 2) as var_95_pct,
    ROUND(max_drawdown * 100, 2) as max_drawdown_pct
FROM market_risk.analytics.computed_metrics
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ticker ORDER BY window_end DESC, computed_at DESC
) = 1
ORDER BY annualized_volatility DESC
LIMIT 25;


-- WIDGET: Risk-Return Scatter (Scatter Plot)
-- Type: Scatter (x=volatility, y=sharpe_ratio, color=sector, label=ticker)
-- Left unlimited: 500 points is exactly the case a scatter handles well, and colouring
-- by sector turns it into the one tile where the full universe earns its keep.
SELECT
    m.ticker,
    u.sector,
    ROUND(m.annualized_volatility, 4) as volatility,
    ROUND(m.sharpe_ratio, 4) as sharpe_ratio,
    ROUND(m.var_95, 4) as var_95
FROM market_risk.analytics.computed_metrics m
LEFT JOIN market_risk.analytics.ticker_universe u ON m.ticker = u.ticker
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY m.ticker ORDER BY m.window_end DESC, m.computed_at DESC
) = 1;


-- WIDGET: Fattest Tails (Grouped Bar)
-- Type: Grouped bar chart showing VaR and CVaR side by side
-- The 25 tickers whose expected shortfall most exceeds their VaR -- i.e. where the tail
-- is worst behaved, which is the comparison this tile exists to make.
SELECT
    ticker,
    ROUND(var_95 * 100, 3) as var_95_pct,
    ROUND(cvar_95 * 100, 3) as cvar_95_pct,
    ROUND(var_99 * 100, 3) as var_99_pct,
    ROUND(cvar_99 * 100, 3) as cvar_99_pct,
    ROUND((cvar_99 - var_99) * 100, 3) as tail_gap_pct
FROM market_risk.analytics.computed_metrics
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ticker ORDER BY window_end DESC, computed_at DESC
) = 1
ORDER BY tail_gap_pct DESC
LIMIT 25;


-- ============================================================================
-- SECTION 4: ROLLING METRICS (Time Series View)
-- ============================================================================
-- Two tiles below take a `ticker` parameter, written as the named marker `:ticker`.
-- That is the syntax the SQL editor and AI/BI dashboards use; a legacy SQL dashboard
-- wants `{{ ticker }}` instead. Substitute if you are still on the legacy type.

-- WIDGET: Rolling Volatility by Sector (Line Chart)
-- Type: Multi-line chart (x=date, y=rolling_vol_pct, color=sector)
-- Aggregated to 11 GICS sectors rather than drawn per ticker: 500 overlapping lines is
-- an opaque band, while sector medians are the level at which the shape means anything.
-- The median resists a single blown-up symbol dragging its sector's line.
SELECT
    u.sector,
    m.date,
    ROUND(MEDIAN(m.rolling_vol_21d) * 100, 2) as rolling_vol_pct,
    COUNT(*) as tickers
FROM market_risk.analytics.v_rolling_metrics m
JOIN market_risk.analytics.ticker_universe u ON m.ticker = u.ticker
WHERE m.date >= DATE_SUB(current_date(), 90)
  AND u.sector IS NOT NULL
GROUP BY u.sector, m.date
ORDER BY u.sector, m.date;


-- WIDGET: Rolling Volatility, Single Ticker (Line Chart)
-- Type: Line chart (x=date, y=rolling_vol_pct) with a `ticker` dashboard parameter
-- The per-ticker drilldown the sector chart above leads to. Parameterized instead of
-- unfiltered because one ticker at a time is the only readable form of this chart.
SELECT
    ticker,
    date,
    ROUND(rolling_vol_21d * 100, 2) as rolling_vol_pct,
    ROUND(daily_return * 100, 3) as daily_return_pct
FROM market_risk.analytics.v_rolling_metrics
WHERE ticker = :ticker
  AND date >= DATE_SUB(current_date(), 365)
ORDER BY date;


-- WIDGET: Return Distribution (Histogram data)
-- Type: Histogram, one ticker via the same `ticker` dashboard parameter
-- A histogram mixing 500 symbols' returns describes the market, not any holding; the
-- parameter keeps it about one name. 365 calendar days is ~252 trading observations.
SELECT
    ticker,
    date,
    ROUND(daily_return * 100, 3) as return_pct
FROM market_risk.analytics.v_rolling_metrics
WHERE ticker = :ticker
  AND date >= DATE_SUB(current_date(), 365)
ORDER BY date;


-- ============================================================================
-- SECTION 5: DATA COVERAGE & INVENTORY
-- ============================================================================

-- WIDGET: Coverage Summary (Counter Tiles)
-- Type: Three counters -- universe size, symbols with data, symbols behind
-- The whole-universe answer to "is coverage healthy", in place of scanning 500 rows.
WITH per_ticker AS (
    SELECT ticker, COUNT(*) as total_records, MIN(date) as first_date, MAX(date) as last_date
    FROM market_risk.analytics.market_prices
    GROUP BY ticker
),
universe AS (
    SELECT COUNT(*) as universe_size
    FROM market_risk.analytics.ticker_universe
    WHERE active
)
SELECT
    u.universe_size,
    COUNT(DISTINCT p.ticker) as tickers_with_data,
    COUNT(DISTINCT CASE WHEN p.last_date < DATE_SUB(current_date(), 4) THEN p.ticker END)
        as tickers_stale,
    MIN(p.first_date) as history_starts,
    SUM(p.total_records) as total_rows
FROM per_ticker p
CROSS JOIN universe u
GROUP BY u.universe_size;


-- WIDGET: Coverage Gaps (Table)
-- Type: Table, the only inventory rows that need action
-- Active symbols with no data at all, or whose last bar predates the most recent one in
-- the table by more than a long weekend. A full 500-row inventory is a scroll, not a
-- signal; sorting by staleness surfaces the symbols a backfill actually missed.
WITH per_ticker AS (
    SELECT
        ticker,
        COUNT(*) as total_records,
        MIN(date) as first_date,
        MAX(date) as last_date,
        MAX(ingested_at) as last_ingested
    FROM market_risk.analytics.market_prices
    GROUP BY ticker
),
latest AS (SELECT MAX(date) as market_date FROM market_risk.analytics.market_prices)
SELECT
    u.ticker,
    u.sector,
    COALESCE(p.total_records, 0) as total_records,
    p.first_date,
    p.last_date,
    DATEDIFF(l.market_date, p.last_date) as days_behind,
    TIMESTAMPDIFF(HOUR, p.last_ingested, current_timestamp()) as hours_since_update
FROM market_risk.analytics.ticker_universe u
CROSS JOIN latest l
LEFT JOIN per_ticker p ON u.ticker = p.ticker
WHERE u.active
  AND (p.ticker IS NULL OR p.last_date < DATE_SUB(l.market_date, 4))
ORDER BY days_behind DESC NULLS FIRST, u.ticker;


-- WIDGET: Daily Ingestion Volume (Area Chart)
-- Type: Area chart (x=ingest_date, y=records_ingested)
-- Totalled rather than split by ticker: a 500-series stacked area is unreadable, and the
-- question this tile answers -- "did the run land the expected volume" -- is about the
-- total. tickers_touched catches a run that wrote plenty of rows for too few symbols.
SELECT
    DATE(ingested_at) as ingest_date,
    COUNT(*) as records_ingested,
    COUNT(DISTINCT ticker) as tickers_touched
FROM market_risk.analytics.market_prices
WHERE ingested_at >= DATE_SUB(current_date(), 30)
GROUP BY DATE(ingested_at)
ORDER BY ingest_date;


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
-- Useful for auditing what data changed.
-- Bounded to the last 3 days by timestamp rather than starting at version 1: a CDF read
-- from the first version replays every change ever made to the table, and after a 20-year
-- backfill that is millions of rows scanned to display 100. The LIMIT trims the result,
-- not the scan.
SELECT
    _change_type,
    _commit_version,
    _commit_timestamp,
    ticker,
    date,
    close,
    volume,
    source
FROM table_changes(
    'market_risk.analytics.market_prices',
    DATE_SUB(current_date(), 3)
)
ORDER BY _commit_timestamp DESC
LIMIT 100;


-- WIDGET: Change Volume by Type (Bar Chart)
-- Type: Bar chart (x=_commit_version, y=changes, color=_change_type)
-- Shows whether a run corrected existing bars (update_preimage/update_postimage pairs)
-- or only appended new ones -- the distinction MERGE exists to make, invisible in the
-- row-level feed above.
SELECT
    _commit_version,
    MIN(_commit_timestamp) as committed_at,
    _change_type,
    COUNT(*) as changes
FROM table_changes(
    'market_risk.analytics.market_prices',
    DATE_SUB(current_date(), 7)
)
GROUP BY _commit_version, _change_type
ORDER BY _commit_version DESC, _change_type;
