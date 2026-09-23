select
    od.order_id,
    od.product_id,
    od.quantity,
    od.unit_price,
    od.discount_amount,
    od.line_total,
    od.line_net,
    od.promo_id,
    od.promo_name,
    od.promo_type,
    od.promo_discount_value,
    od.product_name,
    od.category,
    od.segment,
    od.size,
    od.color,
    ofu.order_date,
    ofu.customer_id,
    ofu.payment_method,
    ofu.payment_value,
    ofu.installments,
    ofu.delivery_days,
    ofu.shipping_fee,
    ofu.order_source,
    ofu.device_type,
    case when r.return_id is not null then 1 else 0 end as return_flag,
    r.return_reason,
    r.return_date,
    r.refund_amount,
    rv.rating,
    rv.review_title,
    g.region,
    g.district,
    g.city
from {{ ref('int_order_details') }} od
join {{ ref('int_order_fulfillment') }} ofu on od.order_id = ofu.order_id
left join {{ ref('stg_returns') }} r
    on od.order_id = r.order_id and od.product_id = r.product_id
left join {{ ref('stg_reviews') }} rv
    on od.order_id = rv.order_id and od.product_id = rv.product_id
left join {{ ref('stg_geography') }} g on ofu.zip = g.zip
