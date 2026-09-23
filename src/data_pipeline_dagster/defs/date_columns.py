"""How each raw table maps onto the daily partition calendar.

Derived from measuring the actual dataset: every table except
order_items/payments (child rows of orders, no date of their own) and
products/geography/promotions/inventory (static/slow-changing dimensions)
carries its own event-date column.
"""

# table_name -> its own date column, used to filter that table's CSV
# directly by partition date.
DIRECT_DATE_COLUMN = {
    "customers": "signup_date",
    "orders": "order_date",
    "shipments": "ship_date",
    "returns": "return_date",
    "reviews": "review_date",
    "web_traffic": "date",
    "sales": "date",
}

# table_name -> the order_id column used to look up the parent order's
# order_date (orders.csv) since these tables have no date column of
# their own.
DERIVED_VIA_ORDER_ID = {
    "order_items": "order_id",
    "payments": "order_id",
}

# sales.csv is the one file whose header casing doesn't match its own
# Postgres/model column name (Date/Revenue/COGS vs date/revenue/cogs,
# see SalesRow's aliases) - override just for reading the raw CSV dict.
# DIRECT_DATE_COLUMN stays the Postgres column name (used for SQL deletes).
CSV_HEADER_OVERRIDE = {
    "sales": "Date",
}

# Static/slow-changing dimension tables: no natural daily grain, loaded
# once as part of the baseline and refreshed on-demand rather than
# partitioned daily.
DIMENSION_TABLES = ["products", "geography", "promotions", "inventory"]
