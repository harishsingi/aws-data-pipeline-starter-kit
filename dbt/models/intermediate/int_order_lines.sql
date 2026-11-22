-- int_order_lines: order lines enriched with customer and product context.
-- This is where business logic starts: joins plus the line_total metric.
-- One row per order line item (grain preserved from stg_orders).

with orders as (

    select * from {{ ref('stg_orders') }}

),

customers as (

    select * from {{ ref('stg_customers') }}

),

products as (

    select * from {{ ref('stg_products') }}

),

joined as (

    select
        o.order_id,
        o.order_ts,
        -- Business date used for daily aggregation downstream.
        cast(o.order_ts as date) as order_date,
        o.customer_id,
        c.full_name,
        c.segment,
        o.product_id,
        p.product_name,
        p.category,
        o.quantity,
        o.unit_price,
        o.discount,
        -- Core revenue metric: net revenue for this line item.
        o.quantity * (o.unit_price - o.discount) as line_total

    from orders as o
    left join customers as c
        on o.customer_id = c.customer_id
    left join products as p
        on o.product_id = p.product_id

)

select * from joined
