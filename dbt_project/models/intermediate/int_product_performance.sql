with product_snapshots as (
    select
        i.snapshot_date,
        i.product_id,
        i.product_name,
        i.category,
        i.segment,
        i.year,
        i.month,
        i.stock_on_hand,
        i.units_received,
        i.units_sold,
        i.stockout_days,
        i.days_of_supply,
        i.fill_rate,
        i.stockout_flag,
        i.overstock_flag,
        i.reorder_flag,
        i.sell_through_rate
    from {{ ref('stg_inventory') }} i
),

product_returns as (
    select
        product_id,
        count(*) as return_count,
        sum(return_quantity) as total_returned_qty,
        sum(refund_amount) as total_refunded
    from {{ ref('stg_returns') }}
    group by 1
),

product_reviews as (
    select
        product_id,
        count(*) as review_count,
        avg(rating) as avg_rating
    from {{ ref('stg_reviews') }}
    group by 1
)

select
    ps.*,
    coalesce(pr.return_count, 0) as return_count,
    coalesce(pr.total_returned_qty, 0) as total_returned_qty,
    coalesce(pr.total_refunded, 0) as total_refunded,
    coalesce(rv.review_count, 0) as review_count,
    rv.avg_rating
from product_snapshots ps
left join product_returns pr on ps.product_id = pr.product_id
left join product_reviews rv on ps.product_id = rv.product_id
