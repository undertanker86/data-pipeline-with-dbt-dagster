"""Shared ingestion logic for both partitioned (daily) and dimension
(unpartitioned) raw-table assets.

Reuses the same Pydantic models and raw-table names that
data_pipeline.ingest already defines, so validation rules never drift
between the plain-script pipeline and the Dagster pipeline.
"""

import csv
from pathlib import Path
from typing import Optional

import pydantic
from sqlalchemy import text
from sqlalchemy.engine import Engine

from data_pipeline.config import DATASET_DIR, DB_SCHEMA, TABLE_REGISTRY
from data_pipeline.ingest import MODEL_MAP, TABLE_NAME_MAP

DAILY_DIR = DATASET_DIR / "daily"


# Cap on how many rejected-row details we keep per partition. Not "all of
# them" (a pathological all-rejected day on a 700k-row table would blow up
# run metadata size) but enough to actually diagnose what's wrong - this
# replaces the old plain-script ingest.py's ingest_errors.csv, which only
# ever held the *last* run's errors (overwritten every time, no per-table/
# per-day history). Metadata here is scoped to the exact partition + asset
# that produced it and kept for every run in the instance's history.
MAX_ERROR_SAMPLES = 25


def _validate_rows(table_name: str, csv_path: Path) -> dict:
    model_name = TABLE_REGISTRY[table_name]
    model_cls = MODEL_MAP[model_name]
    model_fields = list(model_cls.model_fields.keys())

    rows_read = rows_valid = rows_rejected = 0
    batch = []
    sample_errors = []

    if not csv_path.exists():
        return {
            "rows_read": 0, "rows_valid": 0, "rows_rejected": 0,
            "reject_rate": 0.0, "sample_errors": [], "batch": [], "model_fields": model_fields,
        }

    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for rownum, row in enumerate(reader, start=1):
            rows_read += 1
            try:
                parsed = model_cls.model_validate(row)
                batch.append(tuple(getattr(parsed, col) for col in model_fields))
                rows_valid += 1
            except pydantic.ValidationError as e:
                rows_rejected += 1
                if len(sample_errors) < MAX_ERROR_SAMPLES:
                    # e.errors() is structured (field/type/msg per violation),
                    # more useful in a table than the multi-line str(e).
                    first = e.errors()[0]
                    field = ".".join(str(x) for x in first["loc"])
                    sample_errors.append(
                        {"row": rownum, "field": field, "type": first["type"], "msg": first["msg"]}
                    )

    return {
        "rows_read": rows_read,
        "rows_valid": rows_valid,
        "rows_rejected": rows_rejected,
        "reject_rate": (rows_rejected / rows_read) if rows_read else 0.0,
        "sample_errors": sample_errors,
        "batch": batch,
        "model_fields": model_fields,
    }


def _replace_rows(engine: Engine, raw_table: str, model_fields: list, batch: list, delete_clause: str, delete_params: dict) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {DB_SCHEMA}.{raw_table} {delete_clause}"), delete_params)
        if batch:
            placeholders = ", ".join(["%s"] * len(model_fields))
            col_names = ", ".join(model_fields)
            sql = f"INSERT INTO {DB_SCHEMA}.{raw_table} ({col_names}) VALUES ({placeholders})"
            conn.connection.driver_connection.cursor().executemany(sql, batch)


def ingest_daily_partition(table_name: str, engine: Engine, partition_date: str) -> dict:
    """Idempotently (re)load one table's slice for a single day partition
    from its pre-split dataset/daily/<table>/<date>.csv file.
    """
    csv_path = DAILY_DIR / table_name / f"{partition_date}.csv"
    result = _validate_rows(table_name, csv_path)
    raw_table = TABLE_NAME_MAP[table_name]

    from data_pipeline_dagster.defs.date_columns import DIRECT_DATE_COLUMN

    date_col = DIRECT_DATE_COLUMN.get(table_name)
    if date_col is not None:
        delete_clause, delete_params = f"WHERE {date_col} = :d", {"d": partition_date}
    else:
        # order_items/payments: delete by the full set of order_ids the split
        # script grouped into this file (not just the ones that validated),
        # so a row that starts failing validation on reload still gets
        # cleared out instead of left stale.
        order_ids = _order_ids_from_file(csv_path)
        delete_clause, delete_params = "WHERE order_id = ANY(:ids)", {"ids": order_ids or [-1]}

    _replace_rows(engine, raw_table, result["model_fields"], result["batch"], delete_clause, delete_params)
    result.pop("batch")
    result.pop("model_fields")
    return result


def ingest_dimension_table(table_name: str, engine: Engine) -> dict:
    """Full-refresh load for a static/slow-changing dimension table
    (products, geography, promotions, inventory) straight from its
    original master CSV (no daily split needed for these).
    """
    csv_path = DATASET_DIR / f"{table_name}.csv"
    result = _validate_rows(table_name, csv_path)
    raw_table = TABLE_NAME_MAP[table_name]
    _replace_rows(engine, raw_table, result["model_fields"], result["batch"], "WHERE TRUE", {})
    result.pop("batch")
    result.pop("model_fields")
    return result


def error_samples_markdown(sample_errors: list) -> str:
    """Render validate-rejection samples as a markdown table for
    MaterializeResult metadata (renders directly in the Dagster UI)."""
    if not sample_errors:
        return "_no rejected rows_"
    header = "| row | field | type | message |\n|---|---|---|---|"
    rows = "\n".join(
        f"| {e['row']} | `{e['field']}` | {e['type']} | {e['msg']} |" for e in sample_errors
    )
    return f"{header}\n{rows}"


def _order_ids_from_file(csv_path: Path) -> list:
    if not csv_path.exists():
        return []
    with open(csv_path, encoding="utf-8") as f:
        return [int(row["order_id"]) for row in csv.DictReader(f)]
