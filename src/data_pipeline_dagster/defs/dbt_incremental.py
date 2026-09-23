"""The 3 dbt models that need a per-partition run_date threaded into
`dbt build --vars` (rolling-window incremental for late-arriving returns -
see the models themselves for why). Everything else is handled by the
plain declarative dbt/defs.yaml component; this can't be, since a static
YAML component has no way to see context.partition_key.
"""

import json
from pathlib import Path

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from data_pipeline_dagster.defs.partitions import daily_partitions

INCREMENTAL_MODELS = "int_daily_metrics int_return_analysis mart_daily_sales_features"

dbt_project = DbtProject(
    project_dir=Path(__file__).parents[3] / "dbt_project",
)
dbt_project.prepare_if_dev()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    select=INCREMENTAL_MODELS,
    partitions_def=daily_partitions,
    # Lighter than the ingestion assets' retry - a dbt build failure is more
    # often a real SQL/data problem than a transient one, but still worth
    # one retry in case it's a momentary Postgres connection blip.
    retry_policy=dg.RetryPolicy(max_retries=2, delay=30, backoff=dg.Backoff.EXPONENTIAL),
)
def incremental_rolling_window_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    run_date = context.partition_key
    yield from dbt.cli(
        ["build", "--vars", json.dumps({"run_date": run_date})],
        context=context,
    ).stream()


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions(
        assets=[incremental_rolling_window_assets],
        resources={"dbt": DbtCliResource(project_dir=dbt_project)},
    )
