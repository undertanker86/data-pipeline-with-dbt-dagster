# data-pipeline-with-dbt-dagster

Dagster orchestration for an e-commerce dataset: CSV → Pydantic validation → PostgreSQL → dbt → Metabase.

![Architecture](<imgs/archi(1).png>)

## Quick start

```bash
uv sync
docker compose up -d                                  # Postgres + Metabase (see docker-compose.yml)

uv run python scripts/split_dataset_by_date.py         # pre-split CSVs by day (one-time)
uv run python -m data_pipeline.ingest --start-date 2013-01-01 --end-date 2013-02-28   # baseline load
uv run dbt build --project-dir dbt_project --profiles-dir dbt_project                 # build dbt once

uv run dg dev                                          # UI at http://localhost:3000
```

Set `DAGSTER_HOME` once so run history persists across sessions:
```powershell
[System.Environment]::SetEnvironmentVariable('DAGSTER_HOME', 'D:\dagster_data_pipeline\data-pipeline-dagster\.dagster_home', 'User')
```

Copy `.env.example` → `.env` and fill in `GMAIL_*`/`ALERT_EMAIL_TO` to enable the failure-email sensor (optional).

## How it's put together

- **Ingestion (13 assets)**: 9 tables partitioned by day (`orders`, `customers`, ...), 4 dimension tables full-reloaded each run (`products`, `geography`, `promotions`, `inventory`). Idempotent DELETE+INSERT per partition.
- **dbt (23 assets)**: 20 plain models via `DbtProjectComponent`; 3 incremental models (`int_daily_metrics`, `int_return_analysis`, `mart_daily_sales_features`) hand-written as partitioned `@dbt_assets` - `mart_daily_sales_features` reprocesses a rolling 45-day window each run to correctly re-absorb late-arriving returns.
- **Quality**: Asset Checks (`row_count > 0` on dimensions, `reject_rate < 20%` on all ingestion assets) + `RetryPolicy` (3x ingestion, 2x dbt incremental, exponential backoff) + `run_failure_sensor` emailing an HTML failure report.
- **Baseline cutover**: 2013-01-01 → 2013-02-28 loaded once outside Dagster; Dagster takes over daily partitions from 2013-03-01 onward (dataset actually starts 2012-07-04, but `web_traffic.csv` only from 2013-01-01, so the managed range starts there to avoid every table needing a "0 rows today is correct" special case).
- **Visualizing marts**: Metabase at `http://localhost:3001` (port 3001, not 3000 - that's `dg dev`'s UI), point it at Postgres `data_pipeline` / schema `marts`.

Full design rationale, every bug hit while building this (with runnable repro commands), and the reasoning behind each decision above: `README_OLD.md` (kept locally, gitignored - not meant to be re-read cold, more a build log).
