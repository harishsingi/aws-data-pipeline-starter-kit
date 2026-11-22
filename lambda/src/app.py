"""S3 -> validate -> route ingestion Lambda.

Event: S3 ``ObjectCreated`` notifications for objects under
``landing/orders/dt=YYYY-MM-DD/*.jsonl`` in the raw bucket.

For every new file the function:

  1. downloads the object from the raw bucket,
  2. parses it as JSON Lines (blank lines are skipped; a line that is not
     valid JSON becomes an invalid record with error ``invalid_json``),
  3. validates each record with :func:`validate_record`,
  4. writes valid records as JSONL to
     ``validated/orders/dt=<today-UTC>/...`` and invalid records
     (original payload plus a ``_validation_errors`` field) to
     ``quarantine/orders/dt=<today-UTC>/...`` in the same bucket,
     using deterministic output keys derived from the source key,
  5. emits ``ValidRecords`` / ``InvalidRecords`` / ``ProcessedFiles``
     CloudWatch metrics, and returns a summary dict.

Configuration comes from environment variables only -- nothing is
hard-coded:

  * ``RAW_BUCKET``        the raw/data-lake bucket (used as a fallback when
                          the event record carries no bucket name)
  * ``METRICS_NAMESPACE`` CloudWatch namespace for the emitted metrics
                          (default: ``"DataPipelineKit/Ingestion"``)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone

import boto3

# ---------------------------------------------------------------------------
# Structured logging (JSON-ish lines so CloudWatch Logs Insights can query
# them; the guard avoids duplicate handlers on Lambda warm starts)
# ---------------------------------------------------------------------------

logger = logging.getLogger("order_ingestion")
if not logger.handlers:
    _log_handler = logging.StreamHandler()
    _log_handler.setFormatter(
        logging.Formatter(
            '{"timestamp":"%(asctime)s","level":"%(levelname)s",'
            '"logger":"%(name)s","message":"%(message)s"}'
        )
    )
    logger.addHandler(_log_handler)
logger.setLevel(logging.INFO)
logger.propagate = False

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LANDING_PREFIX = "landing/orders/"
VALIDATED_PREFIX = "validated/orders/"
QUARANTINE_PREFIX = "quarantine/orders/"

REQUIRED_FIELDS = (
    "order_id",
    "order_ts",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "discount",
)

METRICS_NAMESPACE_DEFAULT = "DataPipelineKit/Ingestion"

# ---------------------------------------------------------------------------
# AWS client helpers -- kept tiny so unit tests can stub out boto3
# ---------------------------------------------------------------------------


def _s3():
    """Return an S3 client."""
    return boto3.client("s3")


def _cw():
    """Return a CloudWatch client."""
    return boto3.client("cloudwatch")


def _metrics_namespace() -> str:
    """CloudWatch namespace, overridable via the METRICS_NAMESPACE env var."""
    return os.environ.get("METRICS_NAMESPACE", METRICS_NAMESPACE_DEFAULT)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_record(rec: dict) -> list[str]:
    """Validate a single order record.

    Pure function: no I/O and no side effects.

    Returns a list of error codes; an empty list means the record is valid.
    """
    errors: list[str] = []

    if not isinstance(rec, dict):
        return ["not_an_object"]

    # 1. all required fields present (None counts as missing) ----------------
    for field in REQUIRED_FIELDS:
        if field not in rec or rec[field] is None:
            errors.append(f"missing_field:{field}")

    # 2. identifier fields are non-empty strings -----------------------------
    for field in ("order_id", "customer_id", "product_id"):
        value = rec.get(field)
        if value is None:
            continue  # already reported as missing above
        if not isinstance(value, str) or not value.strip():
            errors.append(f"invalid_{field}")

    # 3. order_ts parses as ISO8601 (a trailing 'Z' is tolerated) -------------
    ts = rec.get("order_ts")
    if ts is not None:
        try:
            text = ts if isinstance(ts, str) else str(ts)
            if text.endswith(("Z", "z")):
                text = text[:-1] + "+00:00"
            datetime.fromisoformat(text)
        except (ValueError, TypeError):
            errors.append("invalid_order_ts")

    # 4. quantity: int (bool excluded), 1..1000 ------------------------------
    quantity = rec.get("quantity")
    quantity_ok = False
    if quantity is not None and not isinstance(quantity, bool):
        if isinstance(quantity, int):
            quantity_ok = 1 <= quantity <= 1000
        elif isinstance(quantity, float) and quantity.is_integer():
            # tolerate integral floats (e.g. 3.0) from loose producers
            quantity_ok = 1 <= int(quantity) <= 1000
    if quantity is not None and not quantity_ok:
        errors.append("invalid_quantity")

    # 5. unit_price: number (bool excluded), >= 0 -----------------------------
    unit_price = rec.get("unit_price")
    unit_price_value: float | None = None
    if (
        unit_price is not None
        and not isinstance(unit_price, bool)
        and isinstance(unit_price, (int, float))
        and unit_price >= 0
    ):
        unit_price_value = float(unit_price)
    elif unit_price is not None:
        errors.append("invalid_unit_price")

    # 6. discount: number (bool excluded), 0 <= discount <= unit_price --------
    discount = rec.get("discount")
    if discount is not None:
        discount_ok = (
            not isinstance(discount, bool)
            and isinstance(discount, (int, float))
            and discount >= 0
            and (unit_price_value is None or discount <= unit_price_value)
        )
        if not discount_ok:
            errors.append("invalid_discount")

    return errors


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------


def _source_bucket(record: dict) -> str:
    """Resolve the bucket for an S3 event record.

    Prefers the bucket carried by the event; falls back to the RAW_BUCKET
    environment variable.
    """
    try:
        return record["s3"]["bucket"]["name"]
    except (KeyError, TypeError):
        bucket = os.environ.get("RAW_BUCKET")
        if not bucket:
            raise RuntimeError(
                "Cannot determine bucket: the event record carries no bucket "
                "name and the RAW_BUCKET environment variable is not set."
            )
        return bucket


def _output_key(prefix: str, source_key: str, today: str) -> str:
    """Build a deterministic output key from the source key.

    Example: source ``landing/orders/dt=2026-09-25/orders-0001.jsonl`` becomes
    ``validated/orders/dt=2026-09-25/orders-0001-a1b2c3d4.jsonl``. The short
    sha1 of the full source key keeps keys unique even if two source files
    ever share a basename.
    """
    basename = source_key.rsplit("/", 1)[-1]
    stem = basename.rsplit(".", 1)[0] if "." in basename else basename
    digest = hashlib.sha1(source_key.encode("utf-8")).hexdigest()[:8]
    return f"{prefix}dt={today}/{stem}-{digest}.jsonl"


def _parse_lines(body: str):
    """Yield ``(parsed_record, raw_line)`` for every non-blank line.

    ``parsed_record`` is ``None`` when the line is not valid JSON.
    """
    for line in body.splitlines():
        if not line.strip():
            continue
        try:
            yield json.loads(line), line
        except json.JSONDecodeError:
            yield None, line


def _process_file(record: dict) -> tuple[int, int]:
    """Download, parse, validate and route one S3 object.

    Returns ``(valid_count, invalid_count)``.
    """
    bucket = _source_bucket(record)
    key = record["s3"]["object"]["key"]

    if not key.startswith(LANDING_PREFIX):
        logger.warning(
            "Skipping key outside the landing prefix: bucket=%s key=%s",
            bucket,
            key,
        )
        return 0, 0

    logger.info("Processing file: bucket=%s key=%s", bucket, key)
    obj = _s3().get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    if isinstance(body, bytes):
        body = body.decode("utf-8")

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    valid_lines: list[str] = []
    invalid_lines: list[str] = []
    valid_count = 0
    invalid_count = 0

    for parsed, raw in _parse_lines(body):
        try:
            if parsed is None:
                invalid_count += 1
                invalid_lines.append(
                    json.dumps({"_raw": raw, "_validation_errors": ["invalid_json"]})
                )
                continue
            errors = validate_record(parsed)
            if errors:
                invalid_count += 1
                bad_record = dict(parsed)
                bad_record["_validation_errors"] = errors
                invalid_lines.append(json.dumps(bad_record))
            else:
                valid_count += 1
                valid_lines.append(json.dumps(parsed))
        except Exception:
            # A per-record failure must never kill the batch: quarantine the
            # line and keep going.
            logger.exception("Unexpected error validating a record in key=%s", key)
            invalid_count += 1
            invalid_lines.append(
                json.dumps({"_raw": raw, "_validation_errors": ["processing_error"]})
            )

    s3 = _s3()
    if valid_lines:
        dest = _output_key(VALIDATED_PREFIX, key, today)
        s3.put_object(
            Bucket=bucket,
            Key=dest,
            Body=("\n".join(valid_lines) + "\n").encode("utf-8"),
        )
        logger.info("Wrote %d valid records to s3://%s/%s", valid_count, bucket, dest)
    if invalid_lines:
        dest = _output_key(QUARANTINE_PREFIX, key, today)
        s3.put_object(
            Bucket=bucket,
            Key=dest,
            Body=("\n".join(invalid_lines) + "\n").encode("utf-8"),
        )
        logger.info(
            "Wrote %d invalid records to s3://%s/%s", invalid_count, bucket, dest
        )

    return valid_count, invalid_count


def _emit_metrics(valid: int, invalid: int, files: int) -> None:
    """Emit aggregate CloudWatch metrics for this invocation."""
    namespace = _metrics_namespace()
    try:
        _cw().put_metric_data(
            Namespace=namespace,
            MetricData=[
                {"MetricName": "ValidRecords", "Value": valid, "Unit": "Count"},
                {"MetricName": "InvalidRecords", "Value": invalid, "Unit": "Count"},
                {"MetricName": "ProcessedFiles", "Value": files, "Unit": "Count"},
            ],
        )
        logger.info(
            "Emitted metrics: namespace=%s valid=%d invalid=%d files=%d",
            namespace,
            valid,
            invalid,
            files,
        )
    except Exception:
        # Metrics must never break ingestion; log and move on.
        logger.exception("Failed to emit CloudWatch metrics")


# ---------------------------------------------------------------------------
# Lambda entry point
# ---------------------------------------------------------------------------


def handler(event, context):
    """Handle S3 ObjectCreated notifications.

    Returns a summary dict with per-file details and aggregate counts.
    A per-file failure is logged, metrics are still emitted, and the
    exception is re-raised so the Lambda reports a failure (and S3/Lambda
    retry semantics apply).
    """
    summary = {
        "files_processed": 0,
        "files_failed": 0,
        "valid_records": 0,
        "invalid_records": 0,
        "files": [],
    }

    records = (event or {}).get("Records", [])
    for record in records:
        try:
            key = record.get("s3", {}).get("object", {}).get("key", "<unknown>")
            if not key.startswith(LANDING_PREFIX):
                logger.warning("Skipping record outside landing prefix: key=%s", key)
                continue
            valid_count, invalid_count = _process_file(record)
        except Exception:
            summary["files_failed"] += 1
            logger.exception("Failed to process S3 object; emitting metrics, re-raising")
            _emit_metrics(
                summary["valid_records"],
                summary["invalid_records"],
                summary["files_processed"],
            )
            raise
        summary["files_processed"] += 1
        summary["valid_records"] += valid_count
        summary["invalid_records"] += invalid_count
        summary["files"].append(
            {
                "key": key,
                "valid_records": valid_count,
                "invalid_records": invalid_count,
            }
        )

    _emit_metrics(
        summary["valid_records"],
        summary["invalid_records"],
        summary["files_processed"],
    )
    logger.info("Ingestion complete: %s", json.dumps(summary))
    return summary
