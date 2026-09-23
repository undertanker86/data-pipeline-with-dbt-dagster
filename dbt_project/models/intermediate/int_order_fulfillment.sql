select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.zip,
    o.order_status,
    o.order_source,
    o.device_type,
    p.payment_method,
    p.payment_value,
    p.installments,
    s.ship_date,
    s.delivery_date,
    s.shipping_fee,
    (s.delivery_date - s.ship_date) as delivery_days
from {{ ref('stg_orders') }} o
left join {{ ref('stg_payments') }} p on o.order_id = p.order_id
left join {{ ref('stg_shipments') }} s on o.order_id = s.order_id
