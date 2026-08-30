# Dashboard screenshot capture checklist

Reference for refreshing the images embedded in the main `README.md`.
Drop captures in `docs/img/` using the filenames below, then uncomment the
matching `![...]` lines in the README's *Databricks deployment* section.

## Before capturing

- [ ] **Capture from the personal portfolio workspace only.** The Databricks
      left sidebar renders the full catalog list in the SQL Editor, Catalog
      Explorer and notebook views. Never capture in a workspace whose sidebar
      would expose internal or production catalog names.
- [ ] **Trigger a fresh job run the same day.** The quality scorecard queries
      filter on `check_date = current_date()` and the success-rate tile uses a
      7-day window, so stale data renders those tiles blank.
- [ ] Check each finished image for the workspace URL in the address bar, the
      account/email chrome in the top right, and the sidebar catalog list.
- [ ] Crop tight rather than downscaling a full-screen capture. Target < 300 KB
      per image.

## The captures

| File | Status | Where | What it shows |
| --- | --- | --- | --- |
| `01-workflow-run-graph.png` | captured | Workflows → job → a successful run | Three-task DAG green on serverless, 3m42s, launched by scheduler |
| `02-job-run-history.png` | captured | Workflows → job → Runs tab | Five consecutive scheduled successes plus the earlier debugging failures |
| `03-rolling-volatility-dashboard.png` | captured | Dashboard → Risk Analytics tab | 21-day rolling volatility across ten tickers |
| `04-unity-catalog-lineage.png` | captured | Job run details → Lineage | Three upstream and four downstream tables |
| `05-quality-scorecard.png` | **outstanding** | Dashboard → Operations tab | Ticker × check-name heatmap of 0–1 quality scores |

Known nits to fix on the next pass:

- `03` has a y-axis reading `Sum of rolling_vol_pct`. That is the Databricks
  default aggregation showing through. One row per (ticker, date) means the sum
  equals the value so the chart is correct, but the label reads as an unfixed
  default. Set the aggregation to none, or rename the axis, then re-capture.
- `02` shows the account email in *Creator* and *Run as*. Same address is already
  in `databricks/workflows/market_risk_pipeline.json`, so it is not new exposure,
  but crop it out if you would rather not publish it.

Captions should state what the image *proves*, not what it is — "three-task DAG
on serverless, 2m14s end to end" rather than "workflow screenshot".
