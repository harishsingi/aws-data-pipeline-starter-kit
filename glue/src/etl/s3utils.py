"""S3 control-plane helpers for the Glue job (boto3, no Spark).

The Spark data path (reads/writes through the S3A filesystem) already
retries internally; these helpers cover the boto3 control-plane calls
(HEAD, LIST) that do not.
"""

from __future__ import annotations

import logging
from datetime import datetime

from etl.retry import RetryConfig, retry_with_backoff

logger = logging.getLogger(__name__)

_S3_RETRY = RetryConfig(max_attempts=3, base_delay_seconds=0.5, max_delay_seconds=8.0)


def _s3_client():
    """Return an S3 client (imported lazily so the module loads without boto3)."""
    import boto3

    return boto3.client("s3")


def list_dt_prefixes(s3_client, bucket, prefix):
    """Return sorted list of dt=YYYY-MM-DD values under an S3 prefix."""
    paginator = s3_client.get_paginator("list_objects_v2")
    dts = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            name = cp["Prefix"][len(prefix):].rstrip("/")
            if not name.startswith("dt="):
                continue
            dt_value = name[3:]
            try:
                datetime.strptime(dt_value, "%Y-%m-%d")
            except ValueError:
                continue  # ignore non-date prefixes
            dts.add(dt_value)
    return sorted(dts)


@retry_with_backoff(_S3_RETRY)
def _head_object(s3_client, bucket, key):
    """HEAD one S3 object (retried). Raises when the object is absent."""
    return s3_client.head_object(Bucket=bucket, Key=key)


def object_exists(s3_client, bucket, key):
    """True if the S3 object exists (HEAD request).

    Only a 404 (``NoSuchKey``/``404``) means "absent". Any other failure
    (credentials, throttling exhausted, network) propagates instead of
    being silently treated as "missing", so a broken environment fails
    fast rather than falling back to a stale snapshot.
    """
    try:
        _head_object(s3_client, bucket, key)
        return True
    except Exception as exc:  # noqa: BLE001 - error code decides, not the type
        # _head_object retries, so a persistent 404 surfaces wrapped in
        # RetryExhaustedError; unwrap it to inspect the original error.
        cause = getattr(exc, "last_error", None) or exc
        code = ""
        response = getattr(cause, "response", None)
        if isinstance(response, dict):
            code = str(response.get("Error", {}).get("Code", ""))
        if code in ("404", "NoSuchKey"):
            return False
        raise
