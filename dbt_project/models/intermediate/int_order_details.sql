select
    oi.order_id,
    oi.product_id,
    oi.quantity,
    oi.unit_price,
    oi.discount_amount,
    oi.unit_price * oi.quantity as line_total,
    oi.unit_price * oi.quantity - oi.discount_amount as line_net,
    oi.promo_id,
    oi.promo_id_2,
    pr.product_name,
    pr.category,
    pr.segment,
    pr.size,
    pr.color,
    pm1.promo_name as promo_name,
    pm1.promo_type as promo_type,
    pm1.discount_value as promo_discount_value
from {{ ref('stg_order_items') }} oi
left join {{ ref('stg_products') }} pr on oi.product_id = pr.product_id
left join {{ ref('stg_promotions') }} pm1 on oi.promo_id = pm1.promo_id
