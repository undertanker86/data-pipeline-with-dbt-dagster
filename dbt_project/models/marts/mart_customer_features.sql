select
    co.customer_id,
    co.signup_date,
    co.gender,
    co.age_group,
    co.acquisition_channel,
    co.zip,
    co.city,
    co.region,
    co.district,
    co.first_order_date,
    co.last_order_date,
    co.lifetime_orders,
    (co.first_order_date - co.signup_date) as days_to_first_order,
    coalesce(fin.total_spend, 0) as total_spend,
    coalesce(fin.total_payments, 0) as total_payments,
    coalesce(fin.avg_order_value, 0) as avg_order_value,
    coalesce(fin.last_payment_method, 'unknown') as last_payment_method,
    coalesce(fin.avg_installments, 0) as avg_installments,
    coalesce(ret.total_returns, 0) as total_returns,
    case when co.lifetime_orders > 0
        then coalesce(ret.total_returns, 0)::float / co.lifetime_orders
        else 0 end as return_rate,
    coalesce(rev.avg_rating, 0) as avg_rating,
    coalesce(rev.total_reviews, 0) as total_reviews
from {{ ref('int_customer_orders') }} co
left join (
    select
        customer_id,
        sum(payment_value) as total_spend,
        count(*) as total_payments,
        avg(payment_value) as avg_order_value,
        max(payment_method) as last_payment_method,
        avg(installments) as avg_installments
    from {{ ref('int_order_fulfillment') }}
    group by 1
) fin on co.customer_id = fin.customer_id
left join (
    select
        ofu.customer_id,
        count(distinct r.return_id) as total_returns
    from {{ ref('stg_returns') }} r
    join {{ ref('int_order_fulfillment') }} ofu on r.order_id = ofu.order_id
    group by 1
) ret on co.customer_id = ret.customer_id
left join (
    select
        ofu.customer_id,
        avg(rv.rating) as avg_rating,
        count(*) as total_reviews
    from {{ ref('stg_reviews') }} rv
    join {{ ref('int_order_fulfillment') }} ofu on rv.order_id = ofu.order_id
    group by 1
) rev on co.customer_id = rev.customer_id
