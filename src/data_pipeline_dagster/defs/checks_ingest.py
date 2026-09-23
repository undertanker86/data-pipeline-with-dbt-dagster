import dagster as dg

from data_pipeline.config import DB_SCHEMA
from data_pipeline.ingest import TABLE_NAME_MAP
from data_pipeline_dagster.defs.assets_ingest_dimension import dimension_ingest_assets
from data_pipeline_dagster.defs.assets_ingest_partitioned import partitioned_ingest_assets
from data_pipeline_dagster.defs.resources import PostgresResource

REJECT_RATE_THRESHOLD = 0.20


def _table_name_for(asset_def: dg.AssetsDefinition) -> str:
    # asset key is ["raw", "raw_<table_name>"]
    return asset_def.key.path[-1].removeprefix("raw_")


def _build_row_count_check(asset_def: dg.AssetsDefinition) -> dg.AssetChecksDefinition:
    table_name = _table_name_for(asset_def)
    raw_table = TABLE_NAME_MAP[table_name]

    @dg.asset_check(asset=asset_def, name=f"{table_name}_row_count_positive", blocking=True)
    def _check(postgres: PostgresResource) -> dg.AssetCheckResult:
        with postgres.get_engine().connect() as conn:
            count = conn.exec_driver_sql(f"SELECT COUNT(*) FROM {DB_SCHEMA}.{raw_table}").scalar()
        return dg.AssetCheckResult(passed=count > 0, metadata={"row_count": count})

    return _check


def _build_reject_rate_check(asset_def: dg.AssetsDefinition) -> dg.AssetChecksDefinition:
    table_name = _table_name_for(asset_def)

    @dg.asset_check(asset=asset_def, name=f"{table_name}_reject_rate_below_threshold")
    def _check(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
        event = context.instance.get_latest_materialization_event(asset_def.key)
        if event is None or event.dagster_event is None:
            return dg.AssetCheckResult(passed=True, metadata={"note": "no materialization yet"})
        metadata = event.dagster_event.event_specific_data.materialization.metadata
        reject_rate = metadata.get("reject_rate")
        reject_rate = reject_rate.value if reject_rate is not None else 0.0
        return dg.AssetCheckResult(
            passed=reject_rate < REJECT_RATE_THRESHOLD,
            metadata={"reject_rate": reject_rate, "threshold": REJECT_RATE_THRESHOLD},
        )

    return _check


_all_ingest_assets = [*partitioned_ingest_assets, *dimension_ingest_assets]
ingest_checks = [
    # row_count>0 only makes sense for dimension assets (always full-loaded,
    # should never legitimately be empty). For partitioned assets, 0 rows
    # for a given day can be entirely correct (e.g. web_traffic.csv only
    # starts 2013-01-01 - every partition before that has 0 source rows by
    # design, not a bug) - reject_rate_below_threshold is what actually
    # catches "ingestion silently broke" (rows read but rejected), the
    # failure mode this was built to guard against in the first place.
    *[_build_row_count_check(a) for a in dimension_ingest_assets],
    *[_build_reject_rate_check(a) for a in _all_ingest_assets],
]


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions(asset_checks=ingest_checks)
