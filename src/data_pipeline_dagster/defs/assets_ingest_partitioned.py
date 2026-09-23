import dagster as dg

from data_pipeline_dagster.defs.date_columns import DIRECT_DATE_COLUMN, DERIVED_VIA_ORDER_ID
from data_pipeline_dagster.defs.ingest_lib import ingest_daily_partition, error_samples_markdown
from data_pipeline_dagster.defs.partitions import daily_partitions
from data_pipeline_dagster.defs.resources import PostgresResource

PARTITIONED_TABLES = [*DIRECT_DATE_COLUMN.keys(), *DERIVED_VIA_ORDER_ID.keys()]


def _build_ingest_asset(table_name: str) -> dg.AssetsDefinition:
    @dg.asset(
        name=f"raw_{table_name}",
        key_prefix="raw",
        partitions_def=daily_partitions,
        group_name="ingestion",
        kinds={"python", "postgres"},
        description=f"Daily slice of {table_name}.csv, validated and loaded into raw.raw_{table_name}.",
        # Postgres connection blips are the main realistic transient failure
        # here (CSV read + Pydantic validation don't retry-fix themselves).
        retry_policy=dg.RetryPolicy(max_retries=3, delay=30, backoff=dg.Backoff.EXPONENTIAL),
    )
    def _asset(context: dg.AssetExecutionContext, postgres: PostgresResource) -> dg.MaterializeResult:
        partition_date = context.partition_key
        engine = postgres.get_engine()
        stats = ingest_daily_partition(table_name, engine, partition_date)

        if stats["rows_rejected"]:
            first = stats["sample_errors"][0]
            context.log.warning(
                "%s %s: %s/%s rows rejected. First: row %s, field %s: %s",
                table_name, partition_date, stats["rows_rejected"], stats["rows_read"],
                first["row"], first["field"], first["msg"],
            )

        return dg.MaterializeResult(
            metadata={
                "partition_date": partition_date,
                "rows_read": stats["rows_read"],
                "rows_valid": stats["rows_valid"],
                "rows_rejected": stats["rows_rejected"],
                "reject_rate": dg.MetadataValue.float(round(stats["reject_rate"], 4)),
                "rejected_row_samples": dg.MetadataValue.md(error_samples_markdown(stats["sample_errors"])),
            }
        )

    return _asset


partitioned_ingest_assets = [_build_ingest_asset(t) for t in PARTITIONED_TABLES]


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions(assets=partitioned_ingest_assets)
