{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='date'
  )
}}

-- Rolling-window incremental: daily_returns groups by order_date (the day
-- the order was PLACED), but a return can land 5-31 days later (measured
-- from the real data, median 18d, p90 26d). On every incremental run we
-- therefore re-derive the last 45 days (> the observed max lag, with
-- margin) rather than just "today", so a return that arrives late still
-- gets folded back into the return_count/return_rate of the day its order
-- was placed. 45 days is also enough trailing history for the
-- revenue_7d_avg/revenue_30d_avg window functions below to be correct on
-- the most recent days in this batch.
with daily_base as (
    select
        date,
        revenue,
        cogs,
        order_count,
        unique_customers,
        sessions,
        unique_visitors,
        page_views,
        bounce_rate,
        avg_session_duration_sec,
        traffic_source
    from {{ ref('int_daily_metrics') }}
    {% if is_incremental() %}
    where date >= '{{ var("run_date") }}'::date - interval '45 days'
      and date <= '{{ var("run_date") }}'::date
    {% endif %}
),

daily_promos as (
    select
        o.order_date as date,
        count(distinct oi.promo_id) as active_promo_count
    from {{ ref('stg_orders') }} o
    join {{ ref('stg_order_items') }} oi on o.order_id = oi.order_id
    where oi.promo_id is not null
    {% if is_incremental() %}
    and o.order_date >= '{{ var("run_date") }}'::date - interval '45 days'
    and o.order_date <= '{{ var("run_date") }}'::date
    {% endif %}
    group by 1
),

daily_returns as (
    select
        o.order_date as date,
        count(distinct r.return_id) as return_count
    from {{ ref('stg_returns') }} r
    join {{ ref('stg_orders') }} o on r.order_id = o.order_id
    {% if is_incremental() %}
    where o.order_date >= '{{ var("run_date") }}'::date - interval '45 days'
      and o.order_date <= '{{ var("run_date") }}'::date
    {% endif %}
    group by 1
)

select
    b.date,
    b.revenue,
    b.cogs,
    b.revenue - b.cogs as gross_margin,
    b.order_count,
    b.unique_customers,
    case when b.order_count > 0 then b.revenue / b.order_count else 0 end as avg_order_value,
    coalesce(dp.active_promo_count, 0) as active_promo_count,
    coalesce(dr.return_count, 0) as return_count,
    case when b.order_count > 0 then coalesce(dr.return_count, 0)::float / b.order_count else 0 end as return_rate,
    b.sessions,
    b.unique_visitors,
    b.page_views,
    b.bounce_rate,
    b.avg_session_duration_sec,
    b.traffic_source,
    avg(b.revenue) over (
        order by b.date rows between 6 preceding and current row
    ) as revenue_7d_avg,
    avg(b.revenue) over (
        order by b.date rows between 29 preceding and current row
    ) as revenue_30d_avg
from daily_base b
left join daily_promos dp on b.date = dp.date
left join daily_returns dr on b.date = dr.date
