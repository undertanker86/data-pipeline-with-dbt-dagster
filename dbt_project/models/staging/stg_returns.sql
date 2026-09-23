select
    return_id,
    order_id,
    product_id,
    return_date,
    return_reason,
    return_quantity,
    refund_amount
from {{ source('raw', 'raw_returns') }}
