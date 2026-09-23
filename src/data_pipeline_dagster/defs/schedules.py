import dagster as dg

from data_pipeline_dagster.defs.partitions import daily_partitions

# Both ingestion groups (partitioned "ingestion" - 9 tables - and
# unpartitioned "ingestion_dimensions" - products/geography/promotions/
# inventory) plus everything downstream of them (the auto-generated dbt
# staging/intermediate/marts assets - their source asset keys default to
# AssetKey([source_name, table_name]) = raw/raw_<table>, which matches our
# ingestion asset keys, so the dependency edge is wired automatically with
# no custom DagsterDbtTranslator needed).
#
# Missing "ingestion_dimensions" here previously meant stg_products/
# stg_geography/stg_inventory (and anything downstream of them, e.g.
# mart_product_features) were silently never built by this job - only the
# unpartitioned dbt models reachable from the 9 partitioned tables were.
daily_pipeline_job = dg.define_asset_job(
    name="daily_pipeline_job",
    selection=dg.AssetSelection.groups("ingestion", "ingestion_dimensions").downstream(include_self=True),
    partitions_def=daily_partitions,
)

daily_schedule = dg.build_schedule_from_partitioned_job(
    daily_pipeline_job,
    default_status=dg.DefaultScheduleStatus.STOPPED,
)


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions(jobs=[daily_pipeline_job], schedules=[daily_schedule])
