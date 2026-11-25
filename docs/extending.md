# Extending the Kit

Each recipe below assumes the base pipeline is deployed and running. Change one layer at a time and validate before moving on.

## Add a new source

Suppose you want to ingest `returns` events alongside the existing orders/customers/products flow.

1. **Landing prefix.** Have the new producer write to `s3://data-pipeline-kit-dev-raw/landing/returns/`. The kit's Lambda trigger fires on any `landing/` object.
2. **Extend Lambda validation.** Add the `returns` schema to the validator in the ingest Lambda (new schema branch, same good→`validated/` / bad→`quarantine/` routing contract). Run the Lambda unit tests (`make test-lambda`) to confirm.
3. **Extend the Glue job.** Add a bronze read for `validated/returns/`, clean and dedupe it in silver (new `silver_returns` table registered in the `data_pipeline_kit_dev` catalog).
4. **New silver table → gold.** Decide where it lands: a new fact (`fact_returns`), new columns on an existing dimension, or a bridge table. Keep `dt` partitioning and deterministic `sha2` surrogate keys.
5. **dbt staging.** Add a staging model in the dbt project that selects from the new silver table, then build marts on it (`dbt build --select staging_returns+`). Write to the curated/marts buckets like the existing marts.

The pattern is always the same: landing prefix → Lambda validation → bronze/silver in the Glue job → gold modeling → dbt marts.

## Convert `dim_product` to SCD Type 2

Today `dim_product` is SCD Type 1 (overwrite). To keep product history:

1. In the Glue job's gold stage, change the product merge from an upsert-overwrite to the same versioning logic `dim_customer` uses: on attribute change, close the current row (`effective_to`, `is_current = false`) and insert a new row with a new `sha2(product_id + effective_from)` key.
2. Add `effective_from`, `effective_to`, `is_current` columns to `dim_product` (mirroring `dim_customer`).
3. Update any dbt models that join `fact_orders` → `dim_product` to either filter `is_current = true` or join on the version valid at the fact date — the customer dimension's models show both patterns.
4. Backfill: rerun the gold stage for the historical `dt` range so product history is built from bronze.

## Add streaming (Kinesis + Glue streaming sketch)

For near-real-time ingestion alongside the batch path:

1. **Kinesis Data Stream** (or MSK topic) in front of the existing Lambda: point producers at the stream; keep the S3 `landing/` path as the batch fallback.
2. **Streaming validator.** Extend the ingest Lambda with a Kinesis event source mapping — same validation logic, but micro-batch records are written to a `streaming/` prefix in the raw bucket instead of `validated/`.
3. **Glue streaming job.** Add a new Glue job (streaming type) that reads the `streaming/` prefix (or the Kinesis stream directly) with a checkpoint location in the artifacts bucket, and appends to the silver tables. Use the same `dt` partitioning and `sha2` keys so batch and streaming rows are indistinguishable downstream.
4. **Late data.** Keep the daily batch Glue job as the reconciliation run: it rewrites each `dt` partition with overwrite semantics, so any duplicates or late arrivals from the streaming path are corrected once a day.

Start with the sketch on a dev stack — streaming changes the cost profile (always-on workers) and the failure modes (checkpoints) — before touching production.

## Orchestrate with MWAA / EventBridge Scheduler

The kit is trigger-and-manual by default (Lambda on S3 events; Glue job and dbt run on demand). To schedule it:

- **Simple schedule:** use EventBridge Scheduler to start the Glue job run on a cron (e.g., daily), then trigger the dbt build via a second scheduled Lambda or an SSM/run command step. This is enough for most small pipelines.
- **Full orchestration:** add MWAA (managed Airflow) with a DAG that sequences: sensor for `validated/` partitions → Glue job operator → dbt build task → data-quality check task. Use this when you need retries with dependencies, SLAs, and alerting across steps — not for a single daily job.

## Swap Parquet for Iceberg

The layered tables are written as Parquet with partition overwrite. To move to Apache Iceberg (time travel, schema evolution, MERGE INTO):

1. Enable the Iceberg connector in the Glue job configuration (Glue 4.0 supports it) and in the Athena workgroup.
2. Change gold writes from partition-overwrite Parquet to Iceberg table writes; keep `dt` as a partition field so Athena pruning still works.
3. Register the Iceberg tables in the Glue Data Catalog (the catalog supports Iceberg table types).
4. Update dbt models to target the Iceberg tables; dbt's Athena adapter handles Iceberg materializations.

Iceberg is worth it when you need `MERGE INTO` semantics, safe schema evolution, or snapshot-based rollback. For a write-once-read-many batch pipeline, Parquet with partition overwrite is simpler and cheaper — don't migrate just for fashion.

## Multi-environment (dev / stage / prod via tfvars)

Environments are Terraform variable sets, not separate code:

1. Create `terraform/environments/dev.tfvars`, `stage.tfvars`, `prod.tfvars` setting `project_name`, `environment`, `aws_region`, `glue_worker_type`, `glue_num_workers` per environment. Resource names derive from `project_name` + `environment` (e.g., `data-pipeline-kit-prod-raw`), so stacks don't collide.
2. Keep state separate per environment (separate S3 backends or workspaces) — never share a state file between environments.
3. Apply per environment: `terraform apply -var-file=environments/stage.tfvars`.
4. If you add CI later, have it target prod via secrets/vars; stage can deploy from a `release/*` branch pattern or a manual dispatch — pick one and document it in the workflow file before enabling. (No workflow files ship with this repo.)
5. Promote code, not data: the same commit hash deploys to stage then prod; only the tfvars and secret values differ.
