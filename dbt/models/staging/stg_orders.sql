-- stg_orders: typed, lightly-cleaned view over silver_orders.
-- NO business logic here — only type casts and naming. Derived metrics
-- (line totals, dates) belong in intermediate/marts.

with raw as (

    select * from {{ lake_ref('silver_orders') }}

),

typed as (

    select
        cast(order_id as varchar)      as order_id,
        cast(order_ts as timestamp)    as order_ts,
        cast(customer_id as varchar)   as customer_id,
        cast(product_id as varchar)    as product_id,
        cast(quantity as integer)      as quantity,
        cast(unit_price as decimal(18, 2)) as unit_price,
        cast(discount as decimal(18, 2))   as discount,
        -- Re-derive the date partition from the event timestamp so staging
        -- does not silently trust the ingested `dt` string.
        cast(cast(order_ts as timestamp) as date) as dt

    from raw

)

select * from typed
