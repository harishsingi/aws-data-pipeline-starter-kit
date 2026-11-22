-- stg_customers: typed view over silver_customers.
-- Light renames/casts only; no business logic.

with raw as (

    select * from {{ lake_ref('silver_customers') }}

),

typed as (

    select
        cast(customer_id as varchar)   as customer_id,
        cast(full_name as varchar)     as full_name,
        cast(email as varchar)         as email,
        lower(cast(segment as varchar)) as segment,
        cast(updated_ts as timestamp) as updated_ts

    from raw

)

select * from typed
