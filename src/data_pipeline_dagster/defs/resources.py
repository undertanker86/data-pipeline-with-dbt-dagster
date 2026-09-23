import dagster as dg
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from data_pipeline.config import DATABASE_URL


class PostgresResource(dg.ConfigurableResource):
    """Wraps the same Postgres connection the existing ingest.py/dbt project use."""

    connection_url: str = DATABASE_URL

    def get_engine(self) -> Engine:
        return create_engine(self.connection_url, echo=False)


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(resources={"postgres": PostgresResource()})
