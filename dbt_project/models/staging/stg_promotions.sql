select
    promo_id,
    promo_name,
    promo_type,
    discount_value,
    start_date,
    end_date,
    applicable_category,
    promo_channel,
    stackable_flag,
    min_order_value
from {{ source('raw', 'raw_promotions') }}
