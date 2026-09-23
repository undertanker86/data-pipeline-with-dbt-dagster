# data_pipeline_dagster

Dagster orchestration for an e-commerce retail dataset: CSV → Pydantic validation → PostgreSQL (`raw` schema) → dbt (staging → intermediate → marts). Single-repo mono-repo layout — dbt project and the Pydantic/ingest package both live inside this Dagster project, matching the convention `dg scaffold defs dagster_dbt.DbtProjectComponent` itself suggests.

## Architecture

```
[Baseline - outside Dagster, run once]
  python -m data_pipeline.ingest --start-date 2013-01-01 --end-date 2013-02-28
  → raw.* (13 tables, full DROP SCHEMA CASCADE + reload)

[Dagster - takes over from 2013-03-01, daily partitions through 2022-12-31]

  Ingestion (13 assets, group "raw"):
    9 partitioned by day (group "ingestion"):
      raw_customers (signup_date)     raw_orders (order_date)
      raw_shipments (ship_date)       raw_returns (return_date)
      raw_reviews (review_date)       raw_web_traffic (date)
      raw_sales (date)
      raw_order_items, raw_payments  (no own date - derived via order_id → orders.order_date)
    4 unpartitioned dimension tables (group "ingestion_dimensions", full-reload each run):
      raw_products   raw_geography   raw_promotions   raw_inventory

  dbt (23 assets total, auto-generated from dbt_project/'s manifest.json):
    20 unpartitioned  → defs/dbt/defs.yaml (DbtProjectComponent, plain `dbt build`)
     3 partitioned    → defs/dbt_incremental.py (hand-written @dbt_assets):
       int_daily_metrics, int_return_analysis, mart_daily_sales_features
       (materialized: incremental, need context.partition_key threaded into
       `dbt build --vars '{"run_date": ...}'`, which the static YAML
       component can't do - see "dbt: two asset groups" below)

  Asset Checks:
    row_count > 0            → dimension assets only (4)
    reject_rate < 20%        → all 13 ingestion assets
    (dbt's own not_null/unique tests surface as checks automatically)

  Retries:
    ingestion assets (13)     → max_retries=3, exponential backoff, 30s base
    dbt_incremental.py (3)    → max_retries=2, exponential backoff, 30s base
    (dbt/defs.yaml's 20 unpartitioned models have no retry - the
     declarative YAML component has no field for it)

  Alerts:
    run_failure_sensor → HTML email via Gmail SMTP (defs/sensors.py):
    job/run/partition, a table of every failed step's stack trace,
    and a link back into the Dagster UI
```

**Why the range starts 2013-01-01, not the dataset's actual 2012-07-04**: `web_traffic.csv` only has rows from 2013-01-01 on. Starting the whole pipeline there means every partitioned table has real data for every single day in range - no table needs to special-case "0 rows today is correct, not a bug." This drops ~6 months (Jul-Dec 2012) of otherwise-real orders/customers/sales data.

**Why ingestion is split into two groups**: classic fact-vs-dimension distinction. `orders`, `returns`, etc. are events with their own date, and get replayed one day at a time via `DailyPartitionsDefinition` (so a historical backfill doesn't rescan the whole 10-year CSV on every partition - see `scripts/split_dataset_by_date.py`, which pre-splits each source file into `dataset/daily/<table>/<date>.csv` once). `products`/`geography`/`promotions`/`inventory` are reference tables with no daily grain - "which products exist on 2013-03-05" isn't a meaningful question - so they're unpartitioned and just reloaded in full. **Any job/schedule that selects assets by group must include both** `"ingestion"` and `"ingestion_dimensions"`, or dbt models downstream of the dimension tables (`stg_products`, `stg_geography`, `stg_inventory`, `mart_product_features`) silently never get built - this exact bug happened once in `schedules.py` and was fixed by changing `AssetSelection.groups("ingestion")` to `AssetSelection.groups("ingestion", "ingestion_dimensions")`.

## Project layout

```
data-pipeline-dagster/
  dbt_project/                     dbt project: staging (13 views) → intermediate (6) → marts (4)
  packages/data_pipeline/          Pydantic models + ingest.py, installed as a local editable package
    dataset/                       source CSVs + dataset/daily/<table>/<date>.csv (pre-split, gitignored)
  src/data_pipeline_dagster/
    definitions.py                 @definitions + load_from_defs_folder (auto-discovers everything in defs/)
    defs/
      resources.py                  PostgresResource
      partitions.py                 daily_partitions (2013-03-01 → 2023-01-01, exclusive)
      date_columns.py               which table uses which date column / is derived / is a dimension
      ingest_lib.py                 shared validate+DELETE+INSERT logic for both ingestion asset types
      assets_ingest_partitioned.py  9 partitioned raw_* assets (factory over date_columns.py)
      assets_ingest_dimension.py    4 unpartitioned raw_* assets
      checks_ingest.py              row_count / reject_rate asset checks
      dbt/defs.yaml                 DbtProjectComponent - 20 unpartitioned dbt models
      dbt_incremental.py            3 partitioned dbt models (rolling-window incremental)
      schedules.py                  daily_pipeline_job + daily schedule (default: stopped)
      sensors.py                    email-on-failure sensor
  scripts/split_dataset_by_date.py  one-time preprocessing, run before any backfill
  .env.example                      DB connection + SMTP env var names, no real secrets
  .env                              real values (gitignored) - not committed
```

`data-pipeline-with-dbt-pydantic-fresh` (sibling directory) is the original standalone repo this was copied from — it's untouched and still works independently; this project no longer depends on it at runtime.

## Prerequisites

- [`uv`](https://docs.astral.sh/uv/getting-started/installation/)
- PostgreSQL reachable at the connection string built from `DATA_DB_*` in `.env` (`packages/data_pipeline/src/data_pipeline/config.py` reads them; falls back to `localhost:5432` / db `data_pipeline` / user+password `postgres`/`postgres` if `.env` doesn't set them - which is also exactly what `.env.example` ships as the default). This project was developed against a throwaway Docker container:
  ```bash
  docker run -d --name data-pipeline-pg -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=data_pipeline -p 5432:5432 postgres:16
  ```
- `cp .env.example .env` and fill in at least the `GMAIL_*`/`ALERT_EMAIL_TO` vars if you want the failure-email sensor active (see **Alerts** below). `DATA_DB_*` can be left at the defaults if you're using the container above.

### Local dev environment (`DAGSTER_HOME`)

Set once, applies to every future `dg dev` / `dg launch` / daemon run:
```powershell
[System.Environment]::SetEnvironmentVariable('DAGSTER_HOME', 'D:\dagster_data_pipeline\data-pipeline-dagster\.dagster_home', 'User')
```
Restart any open terminal/VS Code window after running this (User env vars only apply to *new* processes). Without it, every `dg dev` invocation defaults to a fresh throwaway `.tmp_dagster_home_*` folder — run history, sensor state, and schedule status all reset between sessions and don't show up in the UI even though the underlying runs actually happened. This bit us once mid-project: CLI-launched runs succeeded and even fired the failure-email sensor, but were invisible in a `dg dev` UI that had been started without `DAGSTER_HOME` set, because it was looking at a completely different (empty) instance.

## One-time setup

```bash
uv sync

# Split the static dataset into per-day files (needed so partitioned assets
# don't rescan the whole CSV on every run):
uv run python scripts/split_dataset_by_date.py

# Baseline-load the first 2 months directly, before Dagster ever runs.
# This also does DROP SCHEMA CASCADE + recreates every raw.* table.
uv run python -m data_pipeline.ingest --start-date 2013-01-01 --end-date 2013-02-28

# Build dbt once from scratch so every staging/intermediate/marts model
# exists before Dagster's incremental models try to read from them:
uv run dbt build --project-dir dbt_project --profiles-dir dbt_project
```

## Running Dagster

```bash
uv run dg dev
```
Opens the UI at http://localhost:3000. The daily schedule is registered but **stopped by default** (`DefaultScheduleStatus.STOPPED` in `schedules.py`) — turn it on from the UI, or materialize/backfill manually first to see it work before automating it.

To materialize a single partition from the CLI (no UI, runs synchronously so errors show up directly):
```bash
uv run dg launch --assets "raw/raw_orders,raw/raw_sales" --partition "2013-03-05"
```
`dagster job launch` submits to the run queue instead and needs a daemon (`dagster-daemon run`) to actually pick it up — `dg launch` / `dagster asset materialize` execute in-process and are simpler for local testing.

## Alerts

Email-on-failure via `defs/sensors.py` (built-in `dagster.make_email_on_run_failure_sensor`, Gmail SMTP, with a custom `email_body_fn`/`email_subject_fn`). The email is real HTML (the helper already sends `Content-type: text/html` - the default body just doesn't use it): job name, run ID, partition (if any), a table of every failed step's key + full stack trace (via `context.get_step_failure_events()`), and a button linking back to that run in the Dagster UI (`DAGSTER_WEBSERVER_BASE_URL` env var, defaults to `http://localhost:3000`). Needs a Gmail **App Password** (16 chars, requires 2FA enabled first), not the normal account password. Set in `.env` (gitignored):
```
GMAIL_SMTP_USER=you@gmail.com
GMAIL_SMTP_APP_PASSWORD=xxxx xxxx xxxx xxxx
ALERT_EMAIL_TO=you@gmail.com
```
`sensors.py` explicitly `load_dotenv()`s the project-root `.env` by absolute path (`Path(__file__).parents[3] / ".env"`) rather than relying on the default cwd-relative search — `dg`/`dagster` can be invoked from other working directories and would otherwise silently miss the file. If any of the three vars are unset, the sensor logs a warning on failure instead of sending mail (never crashes the pipeline).

**Sensors don't fire on their own** — they need both (a) `default_status` turned on, and (b) the Dagster **daemon** running to actually poll for run-failure events (a CLI-launched run failing does nothing by itself; some process has to notice). For local testing:
```bash
uv run dagster sensor start failure_email_alert -m data_pipeline_dagster.definitions
uv run dagster-daemon run -m data_pipeline_dagster.definitions   # separate terminal, keep running
```
Verified end-to-end: materialized a partition with a deliberately-wrong `postgres` resource `connection_url` (via `--config-json`, no files touched) to force a real failure, and the daemon log showed the sensor reacting within one poll cycle (~30s):
```
Completed a reaction request for run <id>: Sensor "failure_email_alert" acted on run status FAILURE of run <id>.
```

## Ingestion technique

`defs/ingest_lib.py` is what every `raw/raw_*` asset calls into. Two entry points:

- **`ingest_daily_partition(table_name, engine, partition_date)`** — used by the 9 partitioned assets. Reads `dataset/daily/<table>/<partition_date>.csv` (the pre-split file, **not** the master CSV — see below for why), validates every row with the table's real Pydantic model (same `MODEL_MAP`/`TABLE_NAME_MAP` `ingest.py` already defines, imported not re-implemented), then does `DELETE FROM raw.<table> WHERE <date_col> = partition_date` followed by a bulk `INSERT` of the validated rows. For `order_items`/`payments` (no date column of their own), the delete instead targets `WHERE order_id = ANY(<ids>)` using the full set of order_ids the split script grouped into that day's file — not just the ones that passed validation, so a row that starts failing validation on a re-run still gets cleared out instead of left stale.
- **`ingest_dimension_table(table_name, engine)`** — used by the 4 unpartitioned assets. Same validate step, but reads the whole master CSV and does a full `DELETE ... WHERE TRUE` + reinsert every run (there's no meaningful "slice" to delete for a reference table).

Both are **idempotent by construction** — re-running the same partition (or the same dimension table) twice produces identical rows, never duplicates. That's what makes backfills and retries safe to just re-run.

Rejected-row details (row number, field, error type, message - up to 25 per partition) are attached directly to the asset's `MaterializeResult.metadata` (`rejected_row_samples`, rendered as a markdown table in the Dagster UI) instead of a `ingest_errors.csv` that only ever held the last run's errors. Scoped to the exact asset + partition that produced them, kept for every run in the instance's history.

**Why `scripts/split_dataset_by_date.py` has to run first**: a naive implementation would filter the master CSV by date on every partition run. For `order_items.csv` (714k rows) that means re-scanning the whole file on every one of the ~3,700 daily partitions in a historical backfill — computationally infeasible. The script runs once, groups every table's rows by day (`order_items`/`payments` grouped by their *parent order's* `order_date`, via a `customer_id`→date / `order_id`→date lookup built from `orders.csv`), and writes `dataset/daily/<table>/<date>.csv` — so each partition's ingest asset does a single small file read instead of a full-file scan. `sales.csv`'s header (`Date`/`Revenue`/`COGS`, capitalized) needed a one-off `CSV_HEADER_OVERRIDE` in `date_columns.py` since it doesn't match its own Postgres column name (`date`) the way every other table does.

## dbt: two asset groups

- `defs/dbt/defs.yaml` (`DbtProjectComponent`, unpartitioned): 20 of the 23 dbt models — plain `dbt build`, no per-partition state needed.
- `defs/dbt_incremental.py` (hand-written `@dbt_assets`, partitioned by day): `int_daily_metrics`, `int_return_analysis`, `mart_daily_sales_features`. These are `materialized: incremental` and need the Dagster partition's date threaded into `dbt build --vars '{"run_date": ...}'`, which a static YAML component can't do.

  `mart_daily_sales_features` specifically reprocesses a **rolling 45-day window** (not just the current day) on every run, because its `daily_returns` CTE groups by `order_date`, and a return can land 5-31 days after the order (measured from the data, median 18d, p90 26d). Without the rolling window, a late return would never make it back into the return_count of the day its order was placed, and the `revenue_7d_avg`/`revenue_30d_avg` window functions wouldn't have enough trailing rows in the incremental batch either. Verified end-to-end through a real Dagster run: `int_daily_metrics` → 1 row inserted (single day), `mart_daily_sales_features` → 46 rows inserted (the rolling window), all dbt tests passing.

  `dbt_project` has no `packages.yml`/`dbt_utils` dependency (removed — nothing in the models uses it anymore since the upstream fix replaced `dbt_utils.datediff()` with plain date subtraction). Keep it that way: an unused package dependency previously caused every concurrent Dagster subprocess to race on `dbt deps` against the same lock file and time out.

## Known data quality notes (source data, not pipeline bugs)

- **`mart_customer_features.days_to_first_order` can be negative.** Confirmed directly against the raw CSVs (not a bug introduced anywhere in this pipeline): `signup_date` and `order_date` appear to be generated independently in the source dataset, so a customer's first recorded order can predate their recorded signup by weeks. Not yet fixed/flagged — pick one: leave as-is, add a dbt test that flags it without changing values, or null it out in the mart. Discuss before changing mart output.
- Enum values, decimal precision, and the `sales.csv` header casing were all fixed upstream in `data-pipeline-with-dbt-pydantic-fresh` (commit `884b288`) and carried over here — ingest is currently 100% clean (0 rows rejected) across all 13 tables for the 2013+ range.
