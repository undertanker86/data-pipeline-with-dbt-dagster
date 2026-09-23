select
    customer_id,
    zip,
    city,
    signup_date,
    gender,
    age_group,
    acquisition_channel
from {{ source('raw', 'raw_customers') }}
