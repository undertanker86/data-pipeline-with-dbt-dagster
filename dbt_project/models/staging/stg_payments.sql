select
    order_id,
    payment_method,
    payment_value,
    installments
from {{ source('raw', 'raw_payments') }}
