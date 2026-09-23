import argparse
import csv
import sys
from pathlib import Path
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import pydantic
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, Date, Numeric, Text, text
from sqlalchemy.schema import CreateSchema

from .config import DATABASE_URL, DATASET_DIR, DB_SCHEMA, CHUNK_SIZE, TABLE_REGISTRY
from .models import (
    CustomerRow, OrderRow, OrderItemRow, PaymentRow,
    ShipmentRow, ReturnRow, ReviewRow, ProductRow,
    InventoryRow, GeographyRow, PromotionRow,
    WebTrafficRow, SalesRow,
)

MODEL_MAP = {
    "CustomerRow": CustomerRow,
    "OrderRow": OrderRow,
    "OrderItemRow": OrderItemRow,
    "PaymentRow": PaymentRow,
    "ShipmentRow": ShipmentRow,
    "ReturnRow": ReturnRow,
    "ReviewRow": ReviewRow,
    "ProductRow": ProductRow,
    "InventoryRow": InventoryRow,
    "GeographyRow": GeographyRow,
    "PromotionRow": PromotionRow,
    "WebTrafficRow": WebTrafficRow,
    "SalesRow": SalesRow,
}

TABLE_NAME_MAP = {
    "customers":    "raw_customers",
    "orders":       "raw_orders",
    "order_items":  "raw_order_items",
    "payments":     "raw_payments",
    "shipments":    "raw_shipments",
    "returns":       "raw_returns",
    "reviews":      "raw_reviews",
    "products":     "raw_products",
    "inventory":    "raw_inventory",
    "geography":    "raw_geography",
    "promotions":   "raw_promotions",
    "web_traffic":  "raw_web_traffic",
    "sales":        "raw_sales",
}


# Tables whose own CSV column can be used to filter by date (used by the
# optional --start-date/--end-date baseline load). Tables not listed here
# (order_items, payments, products, geography, promotions - no single
# per-row event date) are always loaded in full regardless of date range.
DATE_FIELD_FOR_FILTER = {
    "customers": "signup_date",
    "orders": "order_date",
    "shipments": "ship_date",
    "returns": "return_date",
    "reviews": "review_date",
    "web_traffic": "date",
    "sales": "Date",  # sales.csv header is capitalized, unlike its own model field
    "inventory": "snapshot_date",
}


def clean_value(val):
    """Convert CSV string values to Python types compatible with Pydantic."""
    if val is None or val == "":
        return None
    return val


def parse_row(row_dict, model_cls, rownum):
    cleaned = {}
    for key, val in row_dict.items():
        if val is None or val == "":
            cleaned[key] = None
        else:
            cleaned[key] = val

    try:
        return model_cls.model_validate(cleaned)
    except pydantic.ValidationError as e:
        raise ValueError(f"Row {rownum}: {e}")


def create_tables(engine):
    """Drop and recreate the raw schema and all raw_ tables."""
    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(text(f"DROP SCHEMA IF EXISTS {DB_SCHEMA} CASCADE"))
        conn.execute(CreateSchema(DB_SCHEMA))
        conn.commit()

    metadata = MetaData(schema=DB_SCHEMA)

    Table("raw_customers", metadata,
        Column("customer_id", Integer, primary_key=True),
        Column("zip", Integer, nullable=False),
        Column("city", String, nullable=False),
        Column("signup_date", Date, nullable=False),
        Column("gender", String, nullable=False),
        Column("age_group", String, nullable=False),
        Column("acquisition_channel", String, nullable=False),
    )

    Table("raw_orders", metadata,
        Column("order_id", Integer, primary_key=True),
        Column("order_date", Date, nullable=False),
        Column("customer_id", Integer, nullable=False),
        Column("zip", Integer, nullable=False),
        Column("order_status", String, nullable=False),
        Column("payment_method", String, nullable=False),
        Column("device_type", String, nullable=False),
        Column("order_source", String, nullable=False),
    )

    Table("raw_order_items", metadata,
        Column("order_id", Integer, nullable=False),
        Column("product_id", Integer, nullable=False),
        Column("quantity", Integer, nullable=False),
        Column("unit_price", Numeric(15, 2), nullable=False),
        Column("discount_amount", Numeric(15, 2), nullable=False),
        Column("promo_id", String),
        Column("promo_id_2", String),
    )

    Table("raw_payments", metadata,
        Column("order_id", Integer, primary_key=True),
        Column("payment_method", String, nullable=False),
        Column("payment_value", Numeric(15, 2), nullable=False),
        Column("installments", Integer, nullable=False),
    )

    Table("raw_shipments", metadata,
        Column("order_id", Integer, primary_key=True),
        Column("ship_date", Date, nullable=False),
        Column("delivery_date", Date, nullable=False),
        Column("shipping_fee", Numeric(15, 2), nullable=False),
    )

    Table("raw_returns", metadata,
        Column("return_id", String, primary_key=True),
        Column("order_id", Integer, nullable=False),
        Column("product_id", Integer, nullable=False),
        Column("return_date", Date, nullable=False),
        Column("return_reason", String, nullable=False),
        Column("return_quantity", Integer, nullable=False),
        Column("refund_amount", Numeric(15, 2), nullable=False),
    )

    Table("raw_reviews", metadata,
        Column("review_id", String, primary_key=True),
        Column("order_id", Integer, nullable=False),
        Column("product_id", Integer, nullable=False),
        Column("customer_id", Integer, nullable=False),
        Column("review_date", Date, nullable=False),
        Column("rating", Integer, nullable=False),
        Column("review_title", String, nullable=False),
    )

    Table("raw_products", metadata,
        Column("product_id", Integer, primary_key=True),
        Column("product_name", String, nullable=False),
        Column("category", String, nullable=False),
        Column("segment", String, nullable=False),
        Column("size", String, nullable=False),
        Column("color", String, nullable=False),
        Column("price", Numeric(15, 2), nullable=False),
        Column("cogs", Numeric(15, 2), nullable=False),
    )

    Table("raw_inventory", metadata,
        Column("snapshot_date", Date, nullable=False),
        Column("product_id", Integer, nullable=False),
        Column("stock_on_hand", Integer, nullable=False),
        Column("units_received", Integer, nullable=False),
        Column("units_sold", Integer, nullable=False),
        Column("stockout_days", Integer, nullable=False),
        Column("days_of_supply", Float, nullable=False),
        Column("fill_rate", Float, nullable=False),
        Column("stockout_flag", Integer, nullable=False),
        Column("overstock_flag", Integer, nullable=False),
        Column("reorder_flag", Integer, nullable=False),
        Column("sell_through_rate", Float, nullable=False),
        Column("product_name", String, nullable=False),
        Column("category", String, nullable=False),
        Column("segment", String, nullable=False),
        Column("year", Integer, nullable=False),
        Column("month", Integer, nullable=False),
    )

    Table("raw_geography", metadata,
        Column("zip", Integer, primary_key=True),
        Column("city", String, nullable=False),
        Column("region", String, nullable=False),
        Column("district", String, nullable=False),
    )

    Table("raw_promotions", metadata,
        Column("promo_id", String, primary_key=True),
        Column("promo_name", String, nullable=False),
        Column("promo_type", String, nullable=False),
        Column("discount_value", Numeric(5, 2), nullable=False),
        Column("start_date", Date, nullable=False),
        Column("end_date", Date, nullable=False),
        Column("applicable_category", String),
        Column("promo_channel", String, nullable=False),
        Column("stackable_flag", Integer, nullable=False),
        Column("min_order_value", Numeric(15, 2), nullable=False),
    )

    Table("raw_web_traffic", metadata,
        Column("date", Date, nullable=False),
        Column("sessions", Integer, nullable=False),
        Column("unique_visitors", Integer, nullable=False),
        Column("page_views", Integer, nullable=False),
        Column("bounce_rate", Float, nullable=False),
        Column("avg_session_duration_sec", Float, nullable=False),
        Column("traffic_source", String, nullable=False),
    )

    Table("raw_sales", metadata,
        Column("date", Date, primary_key=True),
        Column("revenue", Numeric(15, 2), nullable=False),
        Column("cogs", Numeric(15, 2), nullable=False),
    )

    metadata.create_all(engine)


def ingest_csv(table_name, csv_path, model_cls, engine, errors_writer, date_range=None):
    table = TABLE_NAME_MAP[table_name]
    model_fields = list(model_cls.model_fields.keys())
    date_field = DATE_FIELD_FOR_FILTER.get(table_name)

    rows_read = 0
    rows_valid = 0
    rows_rejected = 0
    batch = []

    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            if date_range and date_field:
                start, end = date_range
                row_date = row.get(date_field)
                if row_date is None or not (start <= row_date <= end):
                    continue
            rows_read += 1
            try:
                parsed = model_cls.model_validate(row)
                batch.append(tuple(
                    getattr(parsed, col) for col in model_fields
                ))
                rows_valid += 1
            except pydantic.ValidationError as e:
                rows_rejected += 1
                errors_writer.writerow([table_name, rows_read, str(e)])
                continue

            if len(batch) >= CHUNK_SIZE:
                _flush_batch(engine, table, model_fields, batch)
                batch = []

    if batch:
        _flush_batch(engine, table, model_fields, batch)

    return rows_read, rows_valid, rows_rejected


def _flush_batch(engine, table, columns, batch):
    if not batch:
        return
    placeholders = ", ".join(["%s"] * len(columns))
    col_names = ", ".join(columns)
    sql = f"INSERT INTO {DB_SCHEMA}.{table} ({col_names}) VALUES ({placeholders})"
    with engine.begin() as conn:
        conn.connection.driver_connection.cursor().executemany(sql, batch)


def main():
    parser = argparse.ArgumentParser(description="Validate and load CSVs into raw.*")
    parser.add_argument("--start-date", help="Only ingest rows with event date >= this (YYYY-MM-DD). Used for a one-time baseline load; tables with no per-row event date are always loaded in full.")
    parser.add_argument("--end-date", help="Only ingest rows with event date <= this (YYYY-MM-DD).")
    parser.add_argument("--no-reset", action="store_true", help="Skip DROP SCHEMA CASCADE / recreate tables (assumes tables already exist).")
    args = parser.parse_args()

    date_range = (args.start_date, args.end_date) if (args.start_date or args.end_date) else None
    if date_range and (not args.start_date or not args.end_date):
        parser.error("--start-date and --end-date must be given together")

    engine = create_engine(DATABASE_URL, echo=False)

    if not args.no_reset:
        print("Creating / resetting database tables...")
        create_tables(engine)

    errors_path = DATASET_DIR / "ingest_errors.csv"
    errors_file = open(errors_path, "w", newline="")
    errors_writer = csv.writer(errors_file)
    errors_writer.writerow(["table", "row_number", "error"])

    total = {"read": 0, "valid": 0, "rejected": 0}

    for table_name, model_name in TABLE_REGISTRY.items():
        csv_path = DATASET_DIR / f"{table_name}.csv"
        if not csv_path.exists():
            print(f"  [SKIP] {csv_path} not found")
            continue

        model_cls = MODEL_MAP[model_name]
        print(f"Ingesting {table_name}.csv ...", end=" ", flush=True)

        rows_read, rows_valid, rows_rejected = ingest_csv(
            table_name, csv_path, model_cls, engine, errors_writer, date_range=date_range
        )

        total["read"] += rows_read
        total["valid"] += rows_valid
        total["rejected"] += rows_rejected
        print(f"{rows_read} rows -> {rows_valid} ok, {rows_rejected} rejected")

    errors_file.close()

    print(f"\nTotal: {total['read']} rows read, {total['valid']} valid, {total['rejected']} rejected")
    if total["rejected"] > 0:
        print(f"Errors written to {errors_path}")


if __name__ == "__main__":
    main()
