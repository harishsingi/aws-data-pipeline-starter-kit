# Data quality — what GX guards vs. what dbt tests guard

This directory holds the **Great Expectations** (GX 1.x) checks for the AWS
Data Pipeline Starter Kit. There are two quality gates in the pipeline, and
they guard different things on purpose:

| Gate | Tool | Runs when | Guards |
|---|---|---|---|
| **Lake file contracts** | GX (`great_expectations/validate_orders.py`) | Pre/post Glue ingestion, in CI or locally | Raw landing files: required columns present, keys non-null & unique, business ranges, ID formats |
| **Warehouse correctness** | dbt tests (`../dbt`) | `dbt build` | Silver→marts: key uniqueness, referential integrity (`relationships`), dimension vocabularies (`accepted_values`), revenue math (singular SQL test) |

**Why both?** GX catches malformed *files* before a Glue job wastes a run
writing garbage into silver (fail fast at the landing zone). dbt tests catch
*logic* errors in the transformations themselves (joins, aggregations,
metrics). A row can have a valid file shape and still break a join — that's
the dbt layer's job.

## Running the orders validation

```bash
cd data-quality/great_expectations
pip install -r requirements.txt

# JSONL landing files (glob supported, recursive ** works)
python validate_orders.py --input "sample_data/raw/landing/orders/*/*.jsonl"

# or a single Parquet file
python validate_orders.py --input "sample_data/raw/landing/orders/dt=2026-09-24/orders.parquet"
```

Exit code is `0` when every expectation passes, `1` when any expectation
fails (or when no input files match). The script prints a summary: counts of
evaluated/successful/failed expectations plus the failing expectation types
and columns.

## What `orders_suite` checks

Defined in `validate_orders.py::build_suite()` — the file contract every
order landing file must satisfy before ingestion:

* `ExpectColumnValuesToNotBeNull` — `order_id`, `customer_id`, `product_id`, `order_ts`
* `ExpectColumnValuesToBeUnique` — `order_id` (business key; duplicates corrupt order counts)
* `ExpectColumnValuesToBeBetween` — `quantity` in [1, 1000]; `unit_price` in [0, 100000]
* `ExpectColumnValuesToMatchRegex` — `order_id` matches `^ORD-`

Implementation notes:

* Uses an **ephemeral GX context** (`gx.get_context(mode="ephemeral")`) —
  no `great_expectations.yml` project on disk, no Data Docs. Just validate
  and report, which is exactly what CI and pre-Glue checks need.
* Uses the **GX 1.x fluent API**: pandas datasource → dataframe asset →
  whole-dataframe batch definition → `batch.validate(suite)`.

## Adding expectations

1. Add a `gxe.Expect...(...)` instance in `build_suite()` and register it
   with `suite.add_expectation(...)`.
2. Run the script against a known-good sample — new expectations should
   pass there.
3. Run it against a deliberately-broken sample (e.g. a duplicated
   `order_id`) — the new expectation should fail and the exit code
   should be `1`.

## Wiring into CI

Example GitHub Actions step that runs *before* the Glue ingestion job:

```yaml
- name: Validate order landing files
  run: |
    pip install -r data-quality/great_expectations/requirements.txt
    python data-quality/great_expectations/validate_orders.py \
      --input "s3-landing-mirror/raw/landing/orders/*/*.jsonl"
```

Tip: for S3 inputs, sync the landing prefix to the runner first
(`aws s3 sync`), then point `--input` at the local mirror — the script
only reads local files.
