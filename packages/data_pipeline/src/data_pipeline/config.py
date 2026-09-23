import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).parents[2]
DATASET_DIR = PROJECT_ROOT / "dataset"

DB_HOST = os.getenv("DATA_DB_HOST", "localhost")
DB_PORT = int(os.getenv("DATA_DB_PORT", "5432"))
DB_NAME = os.getenv("DATA_DB_NAME", "data_pipeline")
DB_USER = os.getenv("DATA_DB_USER", "postgres")
DB_PASSWORD = os.getenv("DATA_DB_PASSWORD", "postgres")
DB_SCHEMA = os.getenv("DATA_DB_SCHEMA", "raw")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
CHUNK_SIZE = 10_000

TABLE_REGISTRY = {
    "customers": "CustomerRow",
    "orders": "OrderRow",
    "order_items": "OrderItemRow",
    "payments": "PaymentRow",
    "shipments": "ShipmentRow",
    "returns": "ReturnRow",
    "reviews": "ReviewRow",
    "products": "ProductRow",
    "inventory": "InventoryRow",
    "geography": "GeographyRow",
    "promotions": "PromotionRow",
    "web_traffic": "WebTrafficRow",
    "sales": "SalesRow",
}
