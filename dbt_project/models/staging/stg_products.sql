select
    product_id,
    product_name,
    category,
    segment,
    size,
    color,
    price,
    cogs
from {{ source('raw', 'raw_products') }}
