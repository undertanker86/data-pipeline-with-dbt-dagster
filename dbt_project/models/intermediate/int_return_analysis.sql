{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='return_id'
  )
}}

-- Returns are immutable once created (a return_id never gets amended
-- later), so incremental here is a plain append of the day's new returns -
-- no rolling window needed, this is purely a performance optimization.
select
    r.return_id,
    r.order_id,
    r.product_id,
    r.return_date,
    r.return_reason,
    r.return_quantity,
    r.refund_amount,
    oi.quantity as order_quantity,
    oi.unit_price as order_unit_price,
    p.product_name,
    p.category,
    p.segment
from {{ ref('stg_returns') }} r
left join (
    select
        order_id,
        product_id,
        sum(quantity) as quantity,
        avg(unit_price) as unit_price
    from {{ ref('stg_order_items') }}
    group by 1, 2
) oi
    on r.order_id = oi.order_id and r.product_id = oi.product_id
left join {{ ref('stg_products') }} p on r.product_id = p.product_id
{% if is_incremental() %}
where r.return_date = '{{ var("run_date") }}'::date
{% endif %}
