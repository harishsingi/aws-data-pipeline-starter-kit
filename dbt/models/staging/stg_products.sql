-- stg_products: typed view over silver_products.
-- Light renames/casts only; no business logic.

with raw as (

    select * from {{ lake_ref('silver_products') }}

),

typed as (

    select
        cast(product_id as varchar)    as product_id,
        cast(product_name as varchar)  as product_name,
        lower(cast(category as varchar)) as category,
        cast(unit_price as decimal(18, 2)) as unit_price,
        cast(updated_ts as timestamp)  as updated_ts

    from raw

)

select * from typed
