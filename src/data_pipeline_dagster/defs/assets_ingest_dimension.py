import dagster as dg

from data_pipeline_dagster.defs.date_columns import DIMENSION_TABLES
from data_pipeline_dagster.defs.ingest_lib import ingest_dimension_table, error_samples_markdown
from data_pipeline_dagster.defs.resources import PostgresResource


def _build_dimension_asset(table_name: str) -> dg.AssetsDefinition:
    @dg.asset(
        name=f"raw_{table_name}",
        key_prefix="raw",
        group_name="ingestion_dimensions",
        kinds={"python", "postgres"},
        description=f"Static/slow-changing {table_name}.csv, full-refreshed into raw.raw_{table_name}.",
        retry_policy=dg.RetryPolicy(max_retries=3, delay=30, backoff=dg.Backoff.EXPONENTIAL),
    )
    def _asset(context: dg.AssetExecutionContext, postgres: PostgresResource) -> dg.MaterializeResult:
        stats = ingest_dimension_table(table_name, postgres.get_engine())
        if stats["rows_rejected"]:
            first = stats["sample_errors"][0]
            context.log.warning(
                "%s: %s/%s rows rejected. First: row %s, field %s: %s",
                table_name, stats["rows_rejected"], stats["rows_read"],
                first["row"], first["field"], first["msg"],
            )
        return dg.MaterializeResult(
            metadata={
                "rows_read": stats["rows_read"],
                "rows_valid": stats["rows_valid"],
                "rows_rejected": stats["rows_rejected"],
                "reject_rate": dg.MetadataValue.float(round(stats["reject_rate"], 4)),
                "rejected_row_samples": dg.MetadataValue.md(error_samples_markdown(stats["sample_errors"])),
            }
        )

    return _asset


dimension_ingest_assets = [_build_dimension_asset(t) for t in DIMENSION_TABLES]


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions(assets=dimension_ingest_assets)
