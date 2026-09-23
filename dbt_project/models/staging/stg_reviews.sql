select
    review_id,
    order_id,
    product_id,
    customer_id,
    review_date,
    rating,
    review_title
from {{ source('raw', 'raw_reviews') }}
