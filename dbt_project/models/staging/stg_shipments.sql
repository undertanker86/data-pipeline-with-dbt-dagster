select
    order_id,
    ship_date,
    delivery_date,
    shipping_fee
from {{ source('raw', 'raw_shipments') }}
