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

| File | Where | What it should show |
| --- | --- | --- |
| `01-workflow-run-graph.png` | Workflows → job → a successful run | The three-task DAG green end to end, with per-task durations visible |
| `02-job-run-history.png` | Workflows → job → Runs tab | Several dated runs with status and duration, evidencing the schedule |
| `03-risk-metrics-dashboard.png` | Dashboard, Risk Metrics section | Metrics table plus the volatility bar chart and risk-return scatter |
| `04-quality-scorecard.png` | Dashboard, Data Quality section | Ticker × check-name heatmap of 0–1 scores |
| `05-unity-catalog-lineage.png` | Catalog Explorer → `market_prices` → Lineage | Lineage from `market_prices` through to `computed_metrics` |

Captions should state what the image *proves*, not what it is — "three-task DAG
on serverless, 2m14s end to end" rather than "workflow screenshot".
