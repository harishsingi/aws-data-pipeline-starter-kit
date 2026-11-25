# Order Ingestion Lambda

Validates incoming order files and routes each record to either the
**validated** prefix or the **quarantine** prefix of the raw bucket. It is the
first stage of the pipeline: everything downstream (Glue, dbt) reads from the
prefixes this function writes.

## Event flow

1. A producer uploads `landing/orders/dt=YYYY-MM-DD/*.jsonl` to the raw bucket.
2. S3 sends an `ObjectCreated` notification (prefix filter
   `landing/orders/`, configured in `terraform/lambda.tf`) which invokes this
   function.
3. The handler downloads the object, parses it as JSON Lines (blank lines are
   skipped; a line that is not valid JSON becomes an invalid record with error
   `invalid_json`), and validates every record with `validate_record()`.
4. Valid records are written as JSONL to
   `validated/orders/dt=<today-UTC>/<stem>-<sha1>.jsonl`; invalid records are
   written as JSONL to `quarantine/orders/dt=<today-UTC>/<stem>-<sha1>.jsonl`
   with their original payload plus a `_validation_errors` field.
5. CloudWatch metrics `ValidRecords`, `InvalidRecords`, and `ProcessedFiles`
   are emitted, and the handler returns a summary dict.

Reliability notes:

- A per-record failure never kills the batch -- the offending line is
  quarantined with error `processing_error` and processing continues.
- A per-file failure (e.g. the object cannot be read) is logged, metrics are
  still emitted, and the exception is re-raised so the invocation is marked
  failed and normal S3/Lambda retry semantics apply.
- S3 reads/writes and CloudWatch metric emission are retried with
  exponential backoff (3 attempts, see `retry.py`); a persistent error
  still fails the invocation rather than silently dropping the file.
- Output keys are derived deterministically from the source key, so
  re-processing the same file overwrites the same outputs instead of
  duplicating them.

## Module layout

| Module | Contents |
|---|---|
| `app.py` | Handler, S3 wiring, metrics emission |
| `config.py` | Prefixes, field lists, env-based configuration |
| `validation.py` | Pure `validate_record` (mirrors `src/pipeline_kit/validation.py`) |
| `retry.py` | Exponential-backoff retry (mirrors `src/pipeline_kit/retry.py`) |

`validation.py` and `retry.py` are mirrored copies because the Lambda zip
is self-contained; `tests/test_mirrors.py` asserts they agree with the
shared package.

## Environment variables

| Variable            | Required | Default                    | Description                                  |
|---------------------|----------|----------------------------|----------------------------------------------|
| `RAW_BUCKET`        | yes      | --                         | Raw bucket name. Used when the event record carries no bucket name; reads and writes always target the event's bucket. |
| `METRICS_NAMESPACE` | no       | `DataPipelineKit/Ingestion`| CloudWatch namespace for `ValidRecords` / `InvalidRecords` / `ProcessedFiles`. |

## Validation rules

`validate_record(rec)` is a pure function returning a list of error codes
(empty list = valid):

| Check | Rule | Error code(s) |
|-------|------|---------------|
| Required fields | `order_id`, `order_ts`, `customer_id`, `product_id`, `quantity`, `unit_price`, `discount` all present and non-`None` | `missing_field:<name>` |
| Identifiers | `order_id`, `customer_id`, `product_id` are non-empty strings | `invalid_order_id`, `invalid_customer_id`, `invalid_product_id` |
| Timestamp | `order_ts` parses as ISO8601 (`datetime.fromisoformat`; trailing `Z` tolerated) | `invalid_order_ts` |
| Quantity | integer (booleans rejected; integral floats tolerated), 1..1000 | `invalid_quantity` |
| Unit price | number (booleans rejected), `>= 0` | `invalid_unit_price` |
| Discount | number (booleans rejected), `>= 0` and `<= unit_price` | `invalid_discount` |
| Shape | record is a JSON object | `not_an_object` |
| JSON Lines | line parses as JSON | `invalid_json` |

## Local development

Install test dependencies (boto3 is preinstalled in the Lambda Python
runtime, but local runs need it on the path for anything beyond the stubbed
unit tests):

```bash
pip install -r lambda/requirements.txt
```

Run the unit tests from the repo root (stdlib `unittest` only; `boto3` is
stubbed in-process so no AWS credentials are needed):

```bash
python3 -m unittest discover -s lambda/tests
```

## Deploy

### Via Terraform (recommended)

The `terraform/` module provisions the function, its IAM role, the S3
trigger, and the `RAW_BUCKET` / `METRICS_NAMESPACE` environment variables:

```bash
cd terraform
terraform init
terraform apply
```

### Manual update of the function code

The deployment zip contains the whole `lambda/src` directory (Terraform
packages it with `archive_file`; see `terraform/lambda.tf`):

```bash
cd lambda/src
zip -r ../../dist/ingestion.zip .
aws lambda update-function-code \
  --function-name "$FUNCTION_NAME" \
  --zip-file fileb://../../dist/ingestion.zip
```

Replace `$FUNCTION_NAME` with the deployed function name (see
`terraform outputs`). No dependencies need packaging: the runtime already
provides `boto3`.
