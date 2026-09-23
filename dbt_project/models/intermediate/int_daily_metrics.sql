{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='date'
  )
}}

-- Not lag-sensitive (orders/sales/traffic don't get amended after the
-- fact), so incremental here just reprocesses the single day being run
-- rather than a rolling window - purely a performance optimization so a
-- historical backfill doesn't rescan 10 years of stg_orders on every run.
with daily_orders as (
    select
        order_date as date,
        count(distinct order_id) as order_count,
        count(distinct customer_id) as unique_customers
    from {{ ref('stg_orders') }}
    {% if is_incremental() %}
    where order_date = '{{ var("run_date") }}'::date
    {% endif %}
    group by 1
),

daily_sales as (
    select
        date,
        revenue,
        cogs
    from {{ ref('stg_sales') }}
    {% if is_incremental() %}
    where date = '{{ var("run_date") }}'::date
    {% endif %}
),

daily_traffic as (
    select
        date,
        sessions,
        unique_visitors,
        page_views,
        bounce_rate,
        avg_session_duration_sec,
        traffic_source
    from {{ ref('stg_web_traffic') }}
    {% if is_incremental() %}
    where date = '{{ var("run_date") }}'::date
    {% endif %}
)

select
    coalesce(ds.date, orders.date, dt.date) as date,
    coalesce(ds.revenue, 0) as revenue,
    coalesce(ds.cogs, 0) as cogs,
    coalesce(orders.order_count, 0) as order_count,
    coalesce(orders.unique_customers, 0) as unique_customers,
    coalesce(dt.sessions, 0) as sessions,
    coalesce(dt.unique_visitors, 0) as unique_visitors,
    coalesce(dt.page_views, 0) as page_views,
    dt.bounce_rate,
    dt.avg_session_duration_sec,
    dt.traffic_source
from daily_sales ds
full outer join daily_orders orders on ds.date = orders.date
full outer join daily_traffic dt on coalesce(ds.date, orders.date) = dt.date
