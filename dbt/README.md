# pipeline_marts — dbt transformations

Transforms the **silver layer** of the AWS Data Pipeline Starter Kit (Glue
tables `silver_orders`, `silver_customers`, `silver_products`) into
analytics-ready marts:

```
silver_* (Glue) → staging (views: typed, no business logic)
                → intermediate (views: joins + line_total metric)
                → marts (tables: mart_sales_daily, mart_customer_lifetime_value)
```

## Install

```bash
cd dbt
pip install -r requirements.txt
```

## Run locally (no AWS needed)

Local runs use **dbt-duckdb** against the CSV seeds in `seeds/`, which mirror
the silver schemas. The `lake_ref` macro (`macros/lake_ref.sql`) swaps
`source('lake', ...)` for `ref('seed_...')` when `--target local` is used.

```bash
cp profiles.yml.example ~/.dbt/profiles.yml   # first time only
dbt seed --target local    # load the fixture data
dbt build --target local   # models + tests + the singular assertion
dbt docs generate          # optional: browse the data dictionary
```

## Run in production (Athena / Glue)

```bash
cp profiles.yml.example ~/.dbt/profiles.yml   # first time only

export AWS_REGION=us-east-1
export GLUE_DATABASE=data_pipeline_kit_dev   # set by Terraform's glue_database var
# AWS credentials via your usual mechanism (SSO, env vars, instance profile)

dbt build --target prod    # models read Glue tables, marts written back to Glue
```

The `prod` target in `profiles.yml.example` uses the
`data-pipeline-kit-dev-wg` Athena workgroup and stages Athena results under
`s3://data-pipeline-kit-dev-artifacts/dbt-staging/`. Both values come from the
kit's Terraform module.

## Test strategy

| Layer | Mechanism | What it guards |
|---|---|---|
| Staging | Generic tests (`models/staging/schema.yml`) | `unique` + `not_null` on `order_id` / `customer_id` / `product_id`; `relationships` from `int_order_lines` to staging dimensions (no orphan order lines); `accepted_values` on `segment` (consumer/corporate/enterprise) |
| Intermediate | Singular test (`tests/assert_line_total_matches.sql`) | `line_total = quantity * (unit_price - discount)` within $0.01 — the revenue formula generic tests can't express |
| Marts | Generic tests (`models/marts/schema.yml`) | `unique` + `not_null` on mart keys; `relationships` back to staging dimensions |

File-shape and landing-data contracts (before anything reaches silver) are
guarded by the Great Expectations suite in `../data-quality/` — see its
README for the split between GX (lake file contracts) and dbt (warehouse).

## Adding a model

1. Add SQL under `models/<layer>/` using `{{ lake_ref('silver_x') }}` for
   silver reads (staging only) or `{{ ref('stg_x') }}` downstream.
2. Keep casts/type normalization in staging; business logic starts in
   intermediate.
3. Document columns and add tests in the layer's `schema.yml`.
