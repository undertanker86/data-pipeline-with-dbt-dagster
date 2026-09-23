select
    order_id,
    product_id,
    quantity,
    unit_price,
    discount_amount,
    promo_id,
    promo_id_2
from {{ source('raw', 'raw_order_items') }}
