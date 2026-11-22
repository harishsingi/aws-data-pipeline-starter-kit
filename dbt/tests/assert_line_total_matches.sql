-- Singular test: the line_total metric in int_order_lines must equal
-- quantity * (unit_price - discount) within a 1-cent rounding tolerance.
-- Any row returned by this query FAILS the test (dbt convention).
--
-- This guards the core revenue formula in SQL that generic tests cannot
-- express; schema/shape contracts are covered in models/staging/schema.yml.

select
    order_id,
    product_id,
    quantity,
    unit_price,
    discount,
    line_total,
    quantity * (unit_price - discount) as expected_line_total

from {{ ref('int_order_lines') }}

where abs(line_total - quantity * (unit_price - discount)) > 0.01
