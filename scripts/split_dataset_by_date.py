"""One-time preprocessing: split each source CSV into per-day files.

Dagster re-materializes ~3,774 daily partitions during the historical
backfill (2012-09-01 -> 2022-12-31). Filtering the master CSV (up to
714k rows for order_items) on every single partition run would mean
re-scanning the whole file thousands of times. Instead, run this script
once to pre-split every table into dataset/daily/<table>/<date>.csv, so
each Dagster partition just reads its own small file.

Usage (from data-pipeline-dagster/, after `uv sync`):
    uv run python scripts/split_dataset_by_date.py
"""

import csv
from collections import defaultdict
from pathlib import Path

from data_pipeline.config import DATASET_DIR
from data_pipeline_dagster.defs.date_columns import (
    DIRECT_DATE_COLUMN,
    DERIVED_VIA_ORDER_ID,
    CSV_HEADER_OVERRIDE,
)

OUT_DIR = DATASET_DIR / "daily"


def _order_date_lookup() -> dict:
    lookup = {}
    with open(DATASET_DIR / "orders.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            lookup[row["order_id"]] = row["order_date"]
    return lookup


def split_direct(table_name: str, date_col: str) -> None:
    csv_path = DATASET_DIR / f"{table_name}.csv"
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        buckets = defaultdict(list)
        for row in reader:
            buckets[row.get(date_col)].append(row)
    _write_buckets(table_name, fieldnames, buckets)


def split_derived(table_name: str, order_id_col: str, order_dates: dict) -> None:
    csv_path = DATASET_DIR / f"{table_name}.csv"
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        buckets = defaultdict(list)
        for row in reader:
            buckets[order_dates.get(row.get(order_id_col))].append(row)
    _write_buckets(table_name, fieldnames, buckets)


def _write_buckets(table_name: str, fieldnames, buckets: dict) -> None:
    table_dir = OUT_DIR / table_name
    table_dir.mkdir(parents=True, exist_ok=True)
    for date_value, rows in buckets.items():
        if not date_value:
            continue
        out_path = table_dir / f"{date_value}.csv"
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
    print(f"{table_name}: {sum(len(r) for r in buckets.values())} rows -> {len(buckets)} daily files")


def main() -> None:
    for table_name, date_col in DIRECT_DATE_COLUMN.items():
        split_direct(table_name, CSV_HEADER_OVERRIDE.get(table_name, date_col))

    order_dates = _order_date_lookup()
    for table_name, order_id_col in DERIVED_VIA_ORDER_ID.items():
        split_derived(table_name, order_id_col, order_dates)

    print(f"\nDone. Daily files written under {OUT_DIR}")


if __name__ == "__main__":
    main()
