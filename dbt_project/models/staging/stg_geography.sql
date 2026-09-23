select
    zip,
    city,
    region,
    district
from {{ source('raw', 'raw_geography') }}
