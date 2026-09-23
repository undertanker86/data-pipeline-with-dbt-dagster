with product_sales as (
    select
        oi.product_id,
        sum(oi.quantity) as total_quantity_sold,
        sum(oi.line_net) as total_revenue,
        avg(oi.unit_price) as avg_sale_price,
        count(distinct oi.order_id) as total_orders
    from {{ ref('int_order_details') }} oi
    group by 1
),

product_stock_health as (
    select
        product_id,
        avg(stockout_days) as avg_stockout_days,
        avg(fill_rate) as avg_fill_rate,
        sum(stockout_flag) as total_stockout_months,
        sum(overstock_flag) as total_overstock_months,
        sum(reorder_flag) as total_reorder_months
    from {{ ref('int_product_performance') }}
    group by 1
),

product_returns_agg as (
    select
        product_id,
        count(*) as total_returns,
        sum(return_quantity) as total_returned_qty,
        sum(refund_amount) as total_refunded
    from {{ ref('int_return_analysis') }}
    group by 1
),

product_reviews_agg as (
    select
        product_id,
        count(*) as total_reviews,
        avg(rating) as avg_rating
    from {{ ref('stg_reviews') }}
    group by 1
)

select
    p.product_id,
    p.product_name,
    p.category,
    p.segment,
    p.size,
    p.color,
    p.price,
    p.cogs,
    p.price - p.cogs as unit_margin,
    coalesce(ps.total_quantity_sold, 0) as total_quantity_sold,
    coalesce(ps.total_revenue, 0) as total_revenue,
    coalesce(ps.avg_sale_price, 0) as avg_sale_price,
    coalesce(ps.total_orders, 0) as total_orders,
    coalesce(sh.avg_stockout_days, 0) as avg_stockout_days,
    coalesce(sh.avg_fill_rate, 0) as avg_fill_rate,
    coalesce(sh.total_stockout_months, 0) as total_stockout_months,
    coalesce(sh.total_overstock_months, 0) as total_overstock_months,
    coalesce(sh.total_reorder_months, 0) as total_reorder_months,
    coalesce(pr.total_returns, 0) as total_returns,
    coalesce(pr.total_returned_qty, 0) as total_returned_qty,
    coalesce(pr.total_refunded, 0) as total_refunded,
    case when ps.total_quantity_sold > 0
        then coalesce(pr.total_returned_qty, 0)::float / ps.total_quantity_sold
        else 0 end as return_rate,
    coalesce(rv.total_reviews, 0) as total_reviews,
    coalesce(rv.avg_rating, 0) as avg_rating
from {{ ref('stg_products') }} p
left join product_sales ps on p.product_id = ps.product_id
left join product_stock_health sh on p.product_id = sh.product_id
left join product_returns_agg pr on p.product_id = pr.product_id
left join product_reviews_agg rv on p.product_id = rv.product_id
