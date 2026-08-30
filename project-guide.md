# Market Risk Pipeline - Project Guide

## Overview

A financial risk analytics pipeline with **two deployment targets**: a standalone Python/FastAPI service and a **Databricks-native** implementation using Delta Lake, Spark, and SQL Dashboards. Ingests daily OHLCV market data from pluggable sources, computes standard quantitative risk metrics, and serves them with full observability.

### Standalone (FastAPI)
```
source (local CSV / S3 / Yahoo)  ->  validate (Pydantic)  ->  SQL (SQLAlchemy)  ->  metrics  ->  REST API + cache
```

### Databricks
```
source (S3 / Yahoo / Auto Loader)  ->  Spark validation  ->  Delta Lake (MERGE)  ->  Pandas UDF metrics  ->  SQL Dashboard
```

## Project Goals

1. **Ingest market data** from multiple configurable sources (local CSVs, AWS S3, Yahoo Finance, Databricks Volumes)
2. **Validate and clean** incoming data (uppercase tickers, non-negative volume, high >= low)
3. **Compute risk metrics** including VaR, CVaR, volatility, Sharpe ratio, and max drawdown
4. **Persist results** with idempotent upserts (no duplicate rows on re-ingestion)
5. **Serve via REST API** (standalone) or **SQL Dashboard** (Databricks)
6. **Support portfolio-level risk** with diversification ratio for multi-asset portfolios
7. **Monitor pipeline health** with quality scoring, freshness checks, and run auditing

---

## Current Status and Roadmap

**Read this section first.** It is the handover point for a new session. Last
updated 2026-08-30, after item 5 landed.

The project is being worked through a **portfolio-readiness plan**: the code was
already sound, but the repository did not *present* as sound to someone spending
ninety seconds on it. The plan below is ordered by impact per unit of effort, not
by technical interest.

### Verified state

| Check | Value |
|-------|-------|
| Tests | 143 passing |
| Coverage | 84.94% (gate: 80%) |
| `ruff check src/ tests/` | clean |
| `mypy src/` (strict) | clean |
| GitHub Actions CI | **green** on `main` |
| Repo | public, `github.com/aman-shabilin/market-risk-pipeline` |
| Databricks | job runs daily on schedule; last observed run 3m42s, 62,234 rows read / 7,576 written |

### Working conventions

- **Use `.venv-1`, not `.venv`.** `.venv` has no `pytest` installed. Run dev
  commands as `.venv-1/bin/python -m ...`.
- **CI must stay green.** It runs exactly three gates: `pytest --cov`,
  `ruff check src/ tests/`, `mypy src/`. Note the lint gate covers only `src/`
  and `tests/` — `databricks/` and `spark_exercises/` sit outside it and
  currently carry ~146 ruff violations, which is why they do not break CI.
- **Commit messages carry no `Co-Authored-By` trailer.**
- Work happens directly on `main` (solo project, no PR flow).
- Notebook changes cannot be covered by the test suite; verify them with
  `python -m py_compile` and by reading.

### Plan progress

| # | Item | Status |
|---|------|--------|
| 1 | Fix the failing CI (5 ruff errors, 1 mypy error) | **done** — `fbca5ef` |
| 2 | Mermaid architecture diagram + screenshot scaffolding | **done** — `3b7dc04` |
| 3 | Capture and embed Databricks screenshots | **mostly done** — `198ab24`; 4 of 5 captured |
| 4 | Split `spark_exercises/` into its own repository | **not started — next** |
| 5 | Fix `02_compute_risk_metrics.py` (see below) | **done** |
| 6 | Backfill a realistic data volume and publish timings | **not started** |
| 7 | Add a dbt or Airflow layer | **not started** |

Items 1-3 existed because a public repo with a red CI badge and no visual
evidence of the Databricks work reads as unfinished regardless of code quality.
Items 4-7 raise the ceiling rather than repairing the floor.

### Outstanding on item 3

One capture is missing: `docs/img/05-quality-scorecard.png`, the ticker ×
check-name quality heatmap from the dashboard's Operations tab. This is the
project's clearest differentiator, so it is worth getting. Its `![...]`
reference already exists in `README.md` but is commented out, so nothing renders
broken meanwhile. See `docs/CAPTURE_CHECKLIST.md` for the procedure and two
known nits (a `Sum of ...` axis label on the volatility chart, and the account
email visible in the run-history capture).

### Item 5 in detail — what changed

`databricks/notebooks/02_compute_risk_metrics.py` carried one correctness bug and
three quality problems. All five points below are now fixed. The notebook is not
covered by the test suite, so this was verified by `python -m py_compile` and by
reading — **it has not yet been run on Databricks.** Watch the next scheduled run.

1. **Row ordering inside the grouped-map function** (the bug). The function takes
   `dates[0]` / `dates[-1]` as the metric window bounds and runs `np.cumprod` for
   max drawdown, both of which need rows in date order. Sorting upstream did not
   survive the `groupBy` shuffle, so `max_drawdown` and the window bounds could
   vary between runs on identical input. Now `pdf = pdf.sort_values("date")` is
   the first statement in the function body. The upstream `.orderBy("ticker",
   "date")` was dropped, since it bought nothing and cost a full sort.
2. **`PandasUDFType.GROUPED_MAP` (deprecated, removed in Spark 4)** replaced with
   `returns_df.groupBy("ticker").applyInPandas(compute_metrics, schema=metrics_schema)`.
   The plain function keeps the same closure over `risk_free_rate`, and the output
   column order still matches `metrics_schema` positionally, so the result is
   identical under either column-matching mode.
3. **`metrics_clean` is now cached** and its count taken once into
   `metrics_count`. Four actions consume that DataFrame (count, display, MERGE,
   run-audit collect); previously each re-ran the whole DAG, which also meant the
   `computed_at` and `computation_duration_ms` values shown differed from the ones
   merged.
4. **The duplicated rolling logic is down to one definition.** The unused
   `rolling_metrics_df` is gone and the `v_rolling_metrics` view is the single
   source. The view now also excludes the 21-day warmup rows, which the deleted
   PySpark version filtered and the view did not — so the dashboard no longer
   shows a "21-day" volatility computed from two observations. **This changes view
   output:** the earliest 21 rows per ticker disappear.
5. **`F.isnan(...) == False`** replaced with an explicit
   `isNotNull() & ~isnan(...)`, which also drops NULLs.

### Why item 6 matters

The pipeline reads ~62k rows per run across 10 tickers. That is a laptop-sized
problem, which undercuts the Delta MERGE / liquid clustering / Pandas UDF
framing — distributing that work is not yet justified by the data. Backfilling
roughly ten years across ~100 tickers (~250k rows) and publishing before/after
run timings is what turns this from a well-built service into a data engineering
project.

### Confidentiality constraint on screenshots

Captures must come from the personal portfolio workspace only. The Databricks
left sidebar renders the full catalog list in the SQL Editor, Catalog Explorer
and notebook views, so a capture taken in a workspace holding internal or
production catalogs would publish those names to a public repository. Check each
image for the sidebar, the workspace URL and account chrome before committing.

---

## Repository Structure

```
market-risk-pipeline/
├── src/market_risk/           # Core Python package (FastAPI standalone)
│   ├── api/                   #   REST API layer (routes, cache, deps)
│   ├── database/              #   SQLAlchemy models + repository
│   ├── ingestion/             #   Pluggable data sources
│   ├── metrics/               #   Risk metric computation
│   ├── pipeline/              #   Orchestrator + CLI entrypoint
│   ├── schemas/               #   Pydantic validation & response models
│   └── config.py              #   Environment-based settings
├── databricks/                # Databricks-native implementation
│   ├── config/                #   Delta Lake schema setup
│   ├── notebooks/             #   Ingestion, metrics, quality checks
│   ├── sql/                   #   Dashboard queries (15+)
│   └── workflows/             #   Scheduled DAG definition
├── spark_exercises/           # PySpark learning exercises (8 levels)
├── data/
│   ├── sample/                #   Clean sample OHLCV data
│   └── dirty/                 #   Intentionally dirty data for Spark exercises
├── tests/                     # Unit + integration test suite
│   ├── unit/                  #   Isolated metric/schema/repo tests
│   └── integration/           #   End-to-end API + pipeline tests
├── docs/
│   ├── img/                   #   Dashboard + workflow screenshots used by README
│   └── CAPTURE_CHECKLIST.md   #   How to (re)capture those screenshots safely
├── .github/workflows/ci.yml   # GitHub Actions CI pipeline
├── Dockerfile                 # Multi-stage Docker build
├── docker-compose.yml         # App + Redis services
├── pyproject.toml             # Package config, deps, tool settings
├── Makefile                   # Dev commands (install, test, lint, run)
├── .env.example               # All configuration variables documented
└── project-guide.md           # This file
```

---

## Architecture

### Tech Stack

| Layer | Standalone | Databricks |
|-------|-----------|-----------|
| Language | Python 3.11+ | Python (notebooks) + SQL |
| Compute | FastAPI + Uvicorn | Apache Spark (Photon) |
| Storage | SQLAlchemy 2.0 (SQLite / PostgreSQL) | Delta Lake (Unity Catalog) |
| Validation | Pydantic v2 | Spark DataFrame filters + Delta constraints |
| Processing | Pandas, NumPy, SciPy | Pandas UDFs + Spark Window Functions |
| Caching | Redis / in-process TTL | Delta Lake (sub-second on Photon) |
| Data sources | boto3, yfinance | Auto Loader, Volumes, yfinance |
| Orchestration | CLI / API trigger | Databricks Workflows (DAG) |
| Monitoring | API response codes | pipeline_runs + data_quality_scores tables |
| Visualization | Swagger UI (API docs) | Databricks SQL Dashboard |
| Testing | pytest, httpx, moto | Notebook unit tests + quality checks |
| Linting | ruff, mypy --strict | — |
| CI | GitHub Actions | Databricks Repos (Git sync) |
| Deployment | Docker + docker-compose | Workflow JSON (Databricks CLI) |

### Source Layout

```
src/market_risk/
  config.py                 # pydantic-settings: all MR_* env vars
  ingestion/
    base.py                 # DataSource ABC (list_files, read_file)
    local_source.py         # Reads *.csv from a directory
    s3_source.py            # Reads *.csv from S3 bucket via boto3
    yahoo_source.py         # Downloads OHLCV per ticker via yfinance
    __init__.py             # get_data_source() factory
  schemas/
    market_data.py          # MarketDataRow, PricePoint, PriceHistoryResponse
    metrics.py              # RiskMetricsResponse, ReturnsResponse, RollingMetricsPoint, PipelineStatus
    portfolio.py            # PortfolioCreate, PortfolioUpdate, PortfolioResponse, PortfolioRiskResponse
  metrics/
    returns.py              # compute_daily_returns (simple pct_change)
    volatility.py           # annualized_volatility, rolling_volatility
    var.py                  # historical_var, parametric_var
    cvar.py                 # conditional_var (expected shortfall)
    sharpe.py               # sharpe_ratio (annualized, configurable risk-free rate)
    drawdown.py             # max_drawdown (peak-to-trough)
    portfolio.py            # compute_portfolio_risk (weighted returns, diversification ratio)
    service.py              # compute_risk_metrics: single source of truth for metric assembly
    __init__.py             # Public API re-exports
  database/
    engine.py               # Base, get_engine, get_session_factory
    models.py               # MarketPrice, ComputedMetric, Portfolio, PortfolioHolding
    repository.py           # MarketDataRepository (dialect-aware upserts, chunked queries)
  pipeline/
    orchestrator.py         # PipelineOrchestrator.run() + CLI entrypoint (market-risk-ingest)
  api/
    app.py                  # create_app factory, lifespan, CORS middleware
    deps.py                 # FastAPI dependencies: get_repository, get_cache
    cache.py                # CacheBackend ABC, InMemoryCache, RedisCache, build_cache factory
    routes/
      health.py             # GET /health
      ingest.py             # POST /api/v1/ingest/
      metrics.py            # GET /metrics/tickers, GET /metrics/{ticker}, /returns, /rolling
      prices.py             # GET /api/v1/prices/{ticker} (paginated OHLCV history)
      portfolio.py          # CRUD: POST, GET, PUT, DELETE + GET /portfolios/{name}/risk
```

### Database Schema

**`market_prices`** - Daily OHLCV data per ticker
- Columns: `id`, `ticker`, `date`, `open`, `high`, `low`, `close`, `volume`
- Unique constraint: `(ticker, date)` - re-ingesting is idempotent (ON CONFLICT DO NOTHING)

**`computed_metrics`** - Precomputed risk metric snapshots
- Columns: `id`, `ticker`, `computed_at`, `window_start`, `window_end`, `annualized_volatility`, `var_95`, `var_99`, `cvar_95`, `cvar_99`, `sharpe_ratio`, `max_drawdown`
- Unique constraint: `(ticker, window_start, window_end)` - upserts replace existing rows for same window

**`portfolios`** - Named portfolio definitions
- Columns: `id`, `name` (unique), `created_at`
- Has many `portfolio_holdings`

**`portfolio_holdings`** - Ticker weights within a portfolio
- Columns: `id`, `portfolio_id` (FK), `ticker`, `weight`
- Unique constraint: `(portfolio_id, ticker)`

---

## Risk Metrics Computed

All metrics derive from **simple daily returns** of the closing price. Annualization factor: 252 trading days.

| Metric | Definition |
|--------|-----------|
| `annualized_volatility` | std(daily_returns) * sqrt(252) |
| `var_95` / `var_99` | Historical VaR: negative of the 5th/1st percentile of returns (positive = loss) |
| `cvar_95` / `cvar_99` | Expected Shortfall: mean loss in the tail beyond VaR |
| `sharpe_ratio` | (mean_excess_return / std_excess_return) * sqrt(252), risk-free rate = 2% |
| `max_drawdown` | Largest peak-to-trough decline as a positive fraction |
| `parametric_var` | Normal-distribution VaR (available in code, not persisted) |

Portfolio-level metrics additionally include:
| `diversification_ratio` | weighted_avg_individual_vol / portfolio_vol (>1 means diversification benefit) |

Minimum 3 price points required. Non-finite values are rejected (never persisted or served).

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness check |
| `GET` | `/api/v1/metrics/tickers` | List tickers with stored price data (paginated) |
| `GET` | `/api/v1/metrics/{ticker}` | Risk metrics (latest precomputed, or on-demand with `?start_date=&end_date=`) |
| `GET` | `/api/v1/metrics/{ticker}/returns` | Daily return series + stats for distribution charts |
| `GET` | `/api/v1/metrics/{ticker}/rolling` | Rolling volatility and VaR time series (`?window=21`) |
| `GET` | `/api/v1/prices/{ticker}` | Raw OHLCV price history (paginated, date-filterable) |
| `POST` | `/api/v1/ingest/` | Trigger ingestion pass (optional `?ticker=AAPL` filter) |
| `POST` | `/api/v1/portfolios/` | Create a named portfolio with weighted holdings |
| `GET` | `/api/v1/portfolios/` | List all portfolios (paginated) |
| `PUT` | `/api/v1/portfolios/{name}` | Update portfolio holdings |
| `DELETE` | `/api/v1/portfolios/{name}` | Delete a portfolio |
| `GET` | `/api/v1/portfolios/{name}/risk` | Compute portfolio-level risk metrics |

### Metrics Endpoint Behavior
- Without dates: returns the latest precomputed metric row from the pipeline
- With `start_date`/`end_date` (ISO YYYY-MM-DD): computes on-demand from stored prices in that window
- Responses cached by `(ticker, start_date, end_date)` key for `MR_CACHE_TTL_SECONDS`
- Error: 404 (no data), 422 (bad dates or fewer than 3 price points)
- CORS enabled for frontend consumption from any origin

---

## Configuration

All settings via `MR_`-prefixed environment variables or `.env` file (pydantic-settings):

| Variable | Default | Purpose |
|----------|---------|---------|
| `MR_DATABASE_URL` | `sqlite:///./market_risk.db` | SQLAlchemy connection string |
| `MR_DATA_SOURCE` | `local` | `local`, `s3`, or `yahoo` |
| `MR_LOCAL_DATA_PATH` | `./data/sample` | CSV directory for local source |
| `MR_S3_BUCKET` | (empty) | S3 bucket name |
| `MR_S3_PREFIX` | `market-data/` | S3 key prefix |
| `MR_AWS_REGION` | `us-east-1` | AWS region |
| `MR_YAHOO_TICKERS` | `AAPL,MSFT,GOOGL` | Comma-separated ticker list |
| `MR_YAHOO_PERIOD_DAYS` | `365` | Yahoo lookback window |
| `MR_REDIS_URL` | (empty) | Redis URL; empty uses in-memory cache |
| `MR_CACHE_TTL_SECONDS` | `300` | Cache TTL for metric responses |
| `MR_API_HOST` / `MR_API_PORT` | `0.0.0.0` / `8000` | Server bind |
| `MR_RISK_FREE_RATE` | `0.02` | Annualized risk-free rate for Sharpe (configurable) |

---

## Data Sources

Implements a pluggable `DataSource` ABC with `list_files()` and `read_file() -> DataFrame`:

1. **LocalSource** - Reads `*.csv` from a directory on disk
2. **S3Source** - Lists and reads `*.csv` from an S3 bucket/prefix via boto3
3. **YahooFinanceSource** - Downloads adjusted OHLCV per ticker via `yfinance`

Expected CSV schema: `ticker,date,open,high,low,close,volume`

### Validation Rules (Pydantic)
- Ticker: uppercased and stripped
- Volume: must be >= 0
- High must be >= Low
- Invalid rows are skipped (counted as errors), never fail the batch

---

## Pipeline Orchestrator

`PipelineOrchestrator.run(ticker_filter=None)`:

1. Lists all files from the configured data source
2. Reads each file into a DataFrame
3. Validates each row via `MarketDataRow` Pydantic model
4. Optionally filters by ticker
5. Upserts valid price records into `market_prices` (idempotent)
6. For each ticker with stored data, computes and persists the full risk metric set
7. Returns `PipelineResult` with counts and timestamp

CLI entrypoint: `market-risk-ingest` (registered in pyproject.toml as console script).

---

## Caching Strategy

Two backends behind a `CacheBackend` ABC:

- **InMemoryCache** - Dict-based with TTL expiry (default when no Redis configured)
- **RedisCache** - Uses `redis.asyncio` with `SETEX`

Cache keys are `metrics:{ticker}:{start_date}:{end_date}`. TTL is configurable via `MR_CACHE_TTL_SECONDS`.

---

## Database Dialect Support

The repository layer handles three SQL dialects:

1. **SQLite** - Native `ON CONFLICT DO NOTHING` / `ON CONFLICT DO UPDATE`
2. **PostgreSQL** - Native conflict handling via `sqlalchemy.dialects.postgresql.insert`
3. **Generic fallback** - Pre-filters existing records in chunks of 400, then plain insert

This allows swapping the database by changing `MR_DATABASE_URL` without code changes.

---

## Testing

### Structure
```
tests/
  conftest.py               # Shared fixtures (in-memory SQLite, sample prices/returns)
  unit/                     # Isolated metric/schema/repo tests
  integration/              # Full API + pipeline end-to-end tests
```

### Key Patterns
- All tests use in-memory SQLite (`sqlite:///:memory:`)
- Tests pass `_env_file=None` to `Settings()` to avoid reading local `.env`
- Integration tests use `FastAPI.TestClient` (synchronous, from httpx)
- Coverage gate: 80% minimum (`[tool.coverage.report] fail_under = 80`)
- S3 tests use `moto` for mocking

### Running
```bash
make test     # pytest --cov=market_risk -v
make lint     # ruff check + mypy src/
```

---

## CI/CD

GitHub Actions workflow (`.github/workflows/ci.yml`):
- Triggers on push/PR to `main`
- **test job**: checkout, Python 3.12 setup, install deps, pytest with coverage, ruff lint, mypy type check
- **docker job**: builds the Docker image (depends on test passing)

---

## Docker Deployment

**Dockerfile** - Multi-stage build:
- Builder stage: installs the package
- Runtime stage: copies installed packages, source, and data
- CMD: uvicorn with the app factory

**docker-compose.yml** - Two services:
- `app`: the pipeline API (port 8000), depends on redis
- `redis`: Redis 7 Alpine (port 6379)

---

## Databricks Deployment

The `databricks/` directory contains a full Databricks-native implementation of the pipeline, designed to demonstrate platform proficiency.

### Layout

```
databricks/
├── config/
│   └── setup_delta_tables.py          # Unity Catalog schema + Delta table creation
├── notebooks/
│   ├── 01_ingest_market_data.py       # Multi-source ingestion into Delta Lake
│   ├── 02_compute_risk_metrics.py     # Risk metrics via Pandas UDFs
│   └── 03_data_quality_checks.py      # Freshness, completeness, outlier, gap checks
├── sql/
│   └── dashboard_queries.sql          # 15+ queries for Databricks SQL Dashboard
└── workflows/
    └── market_risk_pipeline.json      # Scheduled DAG definition
```

### Databricks Features Demonstrated

| Feature | Implementation |
|---------|---------------|
| **Unity Catalog** | 3-level namespace (`market_risk.analytics.*`), column comments, table properties |
| **Delta Lake MERGE** | Idempotent upserts on `(ticker, date)` for prices, `(ticker, window_start, window_end)` for metrics |
| **Delta Constraints** | `CHECK (high >= low AND low > 0)`, `CHECK (volume >= 0)`, `CHECK (weight > 0)` |
| **Change Data Feed** | Enabled on core tables for audit and downstream CDC consumers |
| **Liquid Clustering** | `CLUSTER BY (ticker, date)` for optimized query performance |
| **Auto Loader** | Incremental file ingestion from cloud storage (processes only new files) |
| **Databricks Volumes** | Unity Catalog managed storage for raw data files |
| **Grouped-Map Pandas Functions** | `groupBy(...).applyInPandas(...)` for scalable per-ticker metric computation across executors |
| **Window Functions** | Rolling volatility, gap detection, lag-based return computation |
| **Databricks Workflows** | DAG orchestration: ingest → quality check → metrics (with retries) |
| **Serverless Compute** | Workflow runs on serverless (no cluster management, auto-scaling) |
| **Parameterized Notebooks** | `dbutils.widgets` for configurable runs without code changes |
| **Pipeline Auditing** | `pipeline_runs` table tracks every execution with status, duration, row counts |
| **Data Quality Framework** | Scoring system (0-1) across 4 dimensions, stored for trending |
| **Scheduled Execution** | Cron trigger on serverless, launched by the scheduler (not manual runs) |
| **SQL Dashboard** | 15+ ready-to-import queries covering ops, quality, metrics, and inventory |

### Delta Lake Tables

| Table | Purpose | Key Features |
|-------|---------|-------------|
| `market_prices` | OHLCV data (Silver) | MERGE upsert, constraints, CDF, liquid clustering |
| `computed_metrics` | Risk metric snapshots (Gold) | MERGE upsert, computation duration tracking |
| `portfolios` | Portfolio definitions | Unique name constraint |
| `portfolio_holdings` | Ticker weights | Weight validation constraint |
| `pipeline_runs` | Execution audit log | Status tracking, error capture, parameters JSON |
| `data_quality_scores` | Quality check results | Per-ticker per-check scoring with JSON details |
| `v_rolling_metrics` | Rolling risk view | SQL view over window function results; single definition of the 21-day window, warmup rows excluded |

### Workflow DAG

```
┌─────────────────────┐
│  ingest_market_data │  (Yahoo/S3/Auto Loader → Delta MERGE)
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  data_quality_checks│  (freshness, completeness, outliers, gaps)
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ compute_risk_metrics│  (Pandas UDF → Delta MERGE)
└─────────────────────┘
```

- Scheduled: weekdays at 6 PM ET (after market close)
- Retries: ingest (2x), quality/metrics (1x)
- Cluster: Photon-enabled, autoscale 1-4 workers
- Notifications: email on failure

### SQL Dashboard Sections

1. **Pipeline Health** — Run history, success rate, last run status
2. **Data Quality Scorecard** — Heatmap of scores, trend over time, failing tickers
3. **Risk Metrics Overview** — Current metrics table, volatility comparison, risk-return scatter
4. **Rolling Metrics** — Time-series volatility, return distribution
5. **Data Coverage** — Inventory per ticker, daily ingestion volume, source breakdown
6. **Delta Operations** — Table history, size detail, change data feed audit

### How to Deploy on Databricks

```bash
# 1. Connect Git repo to Databricks Repos
#    Repos → Add Repo → paste Git URL
#    Repo path: /Repos/Market Data Analysis/market-risk-pipeline/

# 2. Configure Databricks CLI
#    brew tap databricks/tap && brew install databricks
#    databricks configure --profile DEFAULT
#    Host: https://dbc-3302cace-dc1e.cloud.databricks.com

# 3. Run schema setup (one-time)
#    Open databricks/config/setup_delta_tables.py → Run All

# 4. Create the workflow (serverless compute)
#    databricks jobs create --json @databricks/workflows/market_risk_pipeline.json
#    Note: uses serverless environments with yfinance dependency

# 5. Build the SQL Dashboard
#    SQL Editor → New Query → paste sections from databricks/sql/dashboard_queries.sql
#    Pin each query to a new Dashboard

# 6. Trigger first run
#    databricks jobs run-now --job-id <JOB_ID>
```

### Serverless Compute Notes

- Workspace is serverless-only (no custom job clusters)
- Dependencies specified via `environments` block in workflow JSON, not `libraries`
- PySpark RDDs are not supported — use DataFrame `.collect()` instead of `.rdd.flatMap()`
- Persistent views cannot reference temp views — use source table queries directly
- Column `DEFAULT` values require `delta.feature.allowColumnDefaults` (removed from DDL)

---

## Spark Exercises (Supplementary)

The `spark_exercises/` directory contains PySpark data cleansing exercises unrelated to the main pipeline. These are 8 progressive levels teaching DataFrame transformations on intentionally dirty market data (`data/dirty/market_data_dirty.csv`):

1. Read & inspect
2. Standardization (case, trim, types)
3. Null handling
4. Filtering invalid records
5. Deduplication
6. Derived columns & window functions
7. Aggregations & joins
8. Full pipeline (chain everything)

---

## Known Gaps and Technical Debt

### Standalone (FastAPI)
1. **No Alembic migrations** - Schema changes require manual ALTER or fresh DB
2. **Synchronous ingestion** - `POST /api/v1/ingest/` blocks the request for large pulls
3. **No rate limiting on Yahoo** - Fetches one ticker per call with no retry/backoff
4. **No auth/authorization** - API is completely open
5. **Single-process cache** - InMemoryCache doesn't share state across workers

### Databricks
1. **No Structured Streaming for real-time** - Pipeline is batch-only (scheduled daily)
2. **No DLT (Delta Live Tables)** - Could replace notebooks for declarative pipeline with expectations
3. **No MLflow integration** - Could version metric models and track drift
4. **Dashboard requires manual setup** - SQL queries need to be imported manually (no Terraform/API automation yet)
5. **Runs as a user, not a service principal** - The job's `run_as` is the creating user. A service principal would give a non-human audit identity and least-privilege access
6. **Data volume is small** - ~62k rows read per run across 10 tickers, which does not yet justify the distributed machinery
7. **`v_rolling_metrics` recomputes returns on every query** - The view derives daily returns with `LAG` at read time rather than reading the persisted values; materializing it would cut dashboard latency at the cost of another table to keep fresh

---

## How to Run

### Local Development
```bash
make install                   # pip install -e ".[dev]"
cp .env.example .env           # configure data source
make ingest                    # run one ingestion pass
make run                       # uvicorn on http://localhost:8000 with hot-reload
```

### Docker
```bash
make docker-up                 # docker compose up --build (app + Redis)
```

### Sample Workflow
```bash
# 1. Ingest data
curl -X POST http://localhost:8000/api/v1/ingest/

# 2. List available tickers (paginated)
curl "http://localhost:8000/api/v1/metrics/tickers?limit=10&offset=0"

# 3. Get risk metrics for a ticker
curl http://localhost:8000/api/v1/metrics/AAPL

# 4. Get windowed metrics
curl "http://localhost:8000/api/v1/metrics/AAPL?start_date=2024-01-02&end_date=2024-01-31"

# 5. Get price history (for candlestick/line charts)
curl "http://localhost:8000/api/v1/prices/AAPL?limit=100"

# 6. Get daily returns distribution (for histograms)
curl http://localhost:8000/api/v1/metrics/AAPL/returns

# 7. Get rolling metrics (for trend charts)
curl "http://localhost:8000/api/v1/metrics/AAPL/rolling?window=21"

# 8. Create a portfolio
curl -X POST http://localhost:8000/api/v1/portfolios/ \
  -H "Content-Type: application/json" \
  -d '{"name": "tech", "holdings": [{"ticker": "AAPL", "weight": 0.6}, {"ticker": "MSFT", "weight": 0.4}]}'

# 9. Update portfolio holdings
curl -X PUT http://localhost:8000/api/v1/portfolios/tech \
  -H "Content-Type: application/json" \
  -d '{"holdings": [{"ticker": "AAPL", "weight": 0.5}, {"ticker": "MSFT", "weight": 0.3}, {"ticker": "GOOGL", "weight": 0.2}]}'

# 10. Get portfolio risk
curl http://localhost:8000/api/v1/portfolios/tech/risk

# 11. Delete a portfolio
curl -X DELETE http://localhost:8000/api/v1/portfolios/tech
```

---

## Design Decisions

### Standalone (FastAPI)
1. **Single source of truth for metrics** - `metrics/service.py` is used by both the pipeline and the API, preventing drift between precomputed and on-demand paths
2. **Idempotent ingestion** - ON CONFLICT DO NOTHING ensures re-running ingestion is safe
3. **Upsert for metrics** - Same window overwrites existing metrics, keeping the table bounded
4. **Pluggable data sources** - ABC interface allows adding new sources without modifying the pipeline
5. **Validation at the boundary** - Pydantic validates every row before DB insertion; invalid rows are skipped, not fatal
6. **Dialect-aware repository** - Supports SQLite, PostgreSQL, and a generic fallback without code branches in higher layers
7. **Cache abstraction** - Swap between in-memory and Redis via a single env var
8. **Weights normalized** - Portfolio holdings are normalized to sum to 1.0 at creation time
9. **CORS enabled** - Any frontend origin can consume the API without proxy configuration

### Databricks
1. **Delta MERGE over INSERT** - True upsert semantics; handles both new data and corrections in one operation
2. **Liquid Clustering over partitioning** - Better for the query patterns (filter by ticker + date range) without partition skew
3. **Pandas UDFs over Spark native** - Complex financial math (VaR percentile, CVaR tail mean, drawdown) is cleaner in NumPy; Pandas UDFs let Spark distribute it
4. **Separate quality check step** - Runs between ingest and metrics so bad data never feeds into metric computation; quality failures don't block the pipeline (soft gate)
5. **Pipeline run auditing** - Every execution writes to `pipeline_runs` enabling SLA tracking and failure investigation from the dashboard. Because a notebook that raises stops at the failing cell and never reaches its own completion update, each notebook first reaps rows left stranded at `running` by an earlier run of the same `run_type` (safe because the job pins `max_concurrent_runs` to 1). Without this the success-rate tile drifts down while `failed_runs` stays at zero
6. **Quality scoring (0-1)** - Normalized scores enable trending and alerting thresholds without hard-coding check-specific logic
7. **Notebook parameters via widgets** - Same notebook handles dev (manual widget input) and production (workflow-injected parameters)
8. **Change Data Feed enabled** - Downstream consumers (streaming jobs, BI tools) can subscribe to incremental changes without full table scans
9. **Scheduled, not manual** - The job is cron-triggered so runs are reproducible and the audit log reflects real scheduled execution rather than ad-hoc invocations. It currently runs as the creating user; moving to a service principal is listed under known gaps
