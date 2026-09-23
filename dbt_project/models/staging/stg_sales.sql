select
    date,
    revenue,
    cogs
from {{ source('raw', 'raw_sales') }}
