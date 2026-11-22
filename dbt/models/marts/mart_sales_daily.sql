-- mart_sales_daily: daily revenue KPIs. One row per order_date.
-- Feeds dashboards and the daily revenue report.

with lines as (

    select * from {{ ref('int_order_lines') }}

),

daily as (

    select
        order_date,
        count(distinct order_id)    as orders_count,
        sum(line_total)             as total_revenue,
        count(distinct customer_id) as unique_customers

    from lines
    group by order_date

)

select
    order_date,
    orders_count,
    total_revenue,
    -- Guard against division by zero; orders_count > 0 by construction,
    -- but nullif keeps this safe under Athena AND DuckDB semantics.
    total_revenue / nullif(orders_count, 0) as avg_order_value,
    unique_customers

from daily
order by order_date
