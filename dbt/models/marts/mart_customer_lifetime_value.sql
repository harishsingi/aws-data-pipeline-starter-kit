-- mart_customer_lifetime_value: per-customer rollup. One row per customer.
-- Powers segmentation, retention analysis, and LTV reporting.

with lines as (

    select * from {{ ref('int_order_lines') }}

),

customer_rollup as (

    select
        customer_id,
        max(full_name)        as full_name,
        max(segment)          as segment,
        count(distinct order_id) as total_orders,
        sum(line_total)       as lifetime_revenue,
        min(order_ts)         as first_order_ts,
        max(order_ts)         as last_order_ts

    from lines
    group by customer_id

)

select
    customer_id,
    full_name,
    segment,
    total_orders,
    lifetime_revenue,
    first_order_ts,
    last_order_ts

from customer_rollup
