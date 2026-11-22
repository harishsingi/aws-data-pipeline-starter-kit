# Glue ETL Job — `etl_job.py`

The core batch pipeline of the starter kit. One AWS Glue 4.0 job
(Spark 3.3, Python 3.10) that turns validated raw orders plus customer/product
reference snapshots into a queryable bronze → silver → gold lakehouse, registers
every output in the Glue Data Catalog, and enforces data-quality gates before
committing.

## What it does

```
validated/orders ──┐
                   ├─► bronze_orders (audit copy, parquet)
reference/customers├─► silver_orders / silver_customers / silver_products
reference/products ┘         │
                             ▼
                     dim_customer (SCD2)   dim_product (SCD1)   dim_date
                                          │         │               │
                                          └────┬────┴───────┬───────┘
                                               ▼            ▼
                                          fact_orders (star schema)
```

### Stage by stage

1. **Read inputs** — validated orders JSONL for `RUN_DATE` from
   `s3://<RAW>/validated/orders/dt=<RUN_DATE>/*.jsonl`; customer/product
   snapshots from `s3://<RAW>/reference/<kind>/dt=<RUN_DATE>/<kind>.json`.
   If a snapshot is missing for `RUN_DATE`, the job falls back to the latest
   available `dt=` prefix (and refuses to use a snapshot dated *after*
   `RUN_DATE`).
2. **Bronze** — raw validated orders written as Parquet to
   `s3://<RAW>/bronze/orders/dt=<RUN_DATE>/` (immutable audit layer).
   Registered as `bronze_orders`.
3. **Silver orders** — `order_ts` cast to timestamp, quantity/unit_price/discount
   cast to int/decimal(10,2); rows with null `order_id`/`customer_id`/`product_id`
   are quarantined (counted in the logs, dropped, never silently fixed);
   deduplicated by `order_id` keeping the latest `order_ts`. Written with
   dynamic partition overwrite to `s3://<CURATED>/silver/orders/dt=<RUN_DATE>/`.
4. **Silver customers/products** — snapshots deduplicated by id keeping the
   latest `updated_ts`; full overwrite (they are complete snapshots, not deltas).
5. **dim_customer (SCD Type 2)** — full-outer join of the incoming snapshot
   against current dimension rows. Changed `full_name`/`email`/`segment`
   (null-safe comparison) expires the old version
   (`effective_to = RUN_DATE − 1 day`, `is_current = false`) and inserts a new
   version (`effective_from = RUN_DATE`, `effective_to = 9999-12-31`).
   Unchanged rows carry over; new customers are inserted.
   `customer_sk = sha2(customer_id || effective_from)`. First run (no dim table
   yet) starts from an empty dimension.
6. **dim_product (SCD Type 1)** — upsert on `product_id`; changed attributes are
   overwritten in place, new products inserted. `product_sk = sha2(product_id)`.
7. **dim_date** — full calendar year of `RUN_DATE` (~365 rows): `date_sk`
   as `YYYYMMDD` int, year/quarter/month/day, English day name.
8. **fact_orders** — silver orders joined to the *current* `dim_customer` row
   (broadcast), `dim_product` (broadcast), and `dim_date` on
   `date(order_ts)`. `line_total = quantity * (unit_price - discount)`.
   Rows with no dimension match are dropped, orphan counts are logged, and the
   job **fails if more than 5% of rows are orphaned**.
9. **Catalog registration** — every output is registered via
   `CREATE TABLE IF NOT EXISTS ... USING PARQUET [PARTITIONED BY (dt)]`
   followed by best-effort `MSCK REPAIR TABLE`.
10. **Data-quality gates** — `silver_orders` and `fact_orders` row counts > 0,
    no null surrogate keys, `line_total >= 0` everywhere. Any failure raises and
    fails the job (no commit).

## Job arguments

| Argument | Required | Example | Purpose |
|---|---|---|---|
| `--JOB_NAME` | yes | `orders-etl-daily` | Glue job name (used for `job.init`) |
| `--RAW_BUCKET` | yes | `my-raw-bucket` | Raw/validated/bronze bucket (name only, no `s3://`) |
| `--CURATED_BUCKET` | yes | `my-curated-bucket` | Silver outputs |
| `--MARTS_BUCKET` | yes | `my-marts-bucket` | Gold outputs (dims + fact) |
| `--DATABASE` | yes | `orders_mart` | Glue Data Catalog database for all 8 tables |
| `--RUN_DATE` | yes | `2026-09-25` | Processing date, `YYYY-MM-DD` (validated) |

No ARNs, account IDs, or bucket names are hard-coded — the same script runs
unchanged in dev/stage/prod by passing different arguments.

## How Terraform deploys it

The `terraform/` module in this repo provisions the job:

- Uploads `glue/etl_job.py` to the scripts prefix of the artifacts bucket
  (e.g. `s3://<artifacts>/glue/scripts/etl_job.py`).
- Creates an `aws_glue_job` (Glue 4.0, `glueetl`, Python 3) whose
  `command.script_location` points at the uploaded script, with
  `default_arguments` supplying `--RAW_BUCKET`, `--CURATED_BUCKET`,
  `--MARTS_BUCKET`, `--DATABASE`, and `--TempDir`.
- `--RUN_DATE` is supplied per run (schedule trigger or manual invocation),
  not baked into the job definition.
- The module also creates the IAM role (S3 + Glue + CloudWatch Logs access),
  the Glue database, and an EventBridge daily schedule.

## Run it manually

```bash
aws glue start-job-run \
  --job-name orders-etl-daily \
  --arguments '{
    "--RAW_BUCKET": "my-raw-bucket",
    "--CURATED_BUCKET": "my-curated-bucket",
    "--MARTS_BUCKET": "my-marts-bucket",
    "--DATABASE": "orders_mart",
    "--RUN_DATE": "2026-09-25"
  }'
```

Watch progress with:

```bash
aws glue get-job-run --job-name orders-etl-daily --run-id <RUN_ID> --query 'JobRun.{State:JobRunState,Error:ErrorMessage}'
```

## Idempotency

Re-running the same `RUN_DATE` is safe:

- `silver_orders` and `fact_orders` use **dynamic partition overwrite** —
  only the `dt=<RUN_DATE>` partition is replaced; sibling dates are untouched.
- Silver customers/products and all three dimensions are **full overwrites** of
  small datasets; SCD2 logic is deterministic for a given snapshot + run date
  (re-running the same date produces the same versions, because expiry and
  effective dates are derived from `RUN_DATE`, not wall-clock time).
- Bronze is an overwrite of the audit partition for that date.

Caveat: re-running an *older* `RUN_DATE` after a newer one has run will
re-derive SCD2 versions from the older date — by design this is a full-refresh
merge, not an event-sourced history. Backfills should be run in chronological
order.

## Tuning

| Knob | Default in script | When to change |
|---|---|---|
| Glue workers (`WorkerType` / `NumberOfWorkers` in Terraform) | set by the `terraform/` module (start with G.1X × 5–10) | Scale with daily order volume; this job is shuffle-light (broadcast joins for dims) |
| `repartition(8)` on orders/fact writes | 8 | Raise if a single `dt=` partition exceeds ~1–2 GB or write tasks skew |
| `repartition(4)` on dims/silver refs | 4 | Lower to 1–2 for tiny dimensions |
| `repartition(1)` on `dim_date` | 1 | Leave — one small file is ideal |
| Broadcast joins | dims are broadcast | If `dim_customer` ever exceeds broadcast threshold (~10 MB default), remove `broadcast()` and let Spark sort-merge join |
| S3 listing for snapshot fallback | boto3 paginator | Fine at any realistic prefix count |

Use Glue job bookmarks only if you switch the orders source from full daily
drops to append-only streams — with the current `dt=` overwrite layout,
bookmarks add nothing.

## Local syntax check

`awsglue` / `pyspark` are only importable on Glue workers or a Glue dev
endpoint; `py_compile` does not execute imports, so this is enough for CI:

```bash
python3 -m py_compile glue/etl_job.py
```
