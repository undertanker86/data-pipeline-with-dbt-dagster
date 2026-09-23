import dagster as dg

# The dataset actually starts 2012-07-04 for most tables, but web_traffic.csv
# only starts 2013-01-01 - every day before that would be a legitimate
# (correct) 0-row partition for that one table, which isn't worth special-
# casing. So the whole pipeline's range starts 2013-01-01 instead, dropping
# ~6 months (Jul-Dec 2012) of otherwise-real data for the other tables.
#
# Baseline (2013-01-01 -> 2013-02-28) is loaded once outside Dagster, see
# packages/data_pipeline/src/data_pipeline/ingest.py --start-date/--end-date.
# Dagster takes over the day right after the baseline cutoff. end_date is
# exclusive in Dagster, so the last materializable partition is 2022-12-31
# (the last order_date present in the dataset).
daily_partitions = dg.DailyPartitionsDefinition(
    start_date="2013-03-01",
    end_date="2023-01-01",
)
