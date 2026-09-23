select
    order_id,
    order_date,
    customer_id,
    zip,
    order_status,
    payment_method,
    device_type,
    order_source
from {{ source('raw', 'raw_orders') }}
