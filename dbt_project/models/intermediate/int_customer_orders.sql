select
    c.customer_id,
    c.signup_date,
    c.gender,
    c.age_group,
    c.acquisition_channel,
    c.zip,
    c.city,
    g.region,
    g.district,
    min(o.order_date) as first_order_date,
    max(o.order_date) as last_order_date,
    count(distinct o.order_id) as lifetime_orders
from {{ ref('stg_customers') }} c
left join {{ ref('stg_orders') }} o on c.customer_id = o.customer_id
left join {{ ref('stg_geography') }} g on c.zip = g.zip
group by 1, 2, 3, 4, 5, 6, 7, 8, 9
