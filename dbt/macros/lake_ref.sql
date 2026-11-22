{#
  lake_ref(table_name)

  Returns the correct relation for a silver table depending on the target.

  Local dev pattern (--target local, dbt-duckdb):
      Models read from CSV seeds (seeds/seed_silver_*.csv) so the full
      pipeline builds on a laptop with zero AWS access. Seeds are named
      `seed_<table_name>` so `lake_ref('silver_orders')` resolves to
      `ref('seed_silver_orders')`.

  Prod (--target prod, dbt-athena):
      Models read the real Glue tables via the `lake` source definition
      (models/sources.yml), which points at the Glue database configured
      through the GLUE_DATABASE env var.

  Usage:  {{ lake_ref('silver_orders') }}
#}
{% macro lake_ref(table_name) %}
  {% if target.name == 'local' %}
    {{ ref('seed_' ~ table_name) }}
  {% else %}
    {{ source('lake', table_name) }}
  {% endif %}
{% endmacro %}
