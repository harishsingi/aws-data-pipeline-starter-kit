"""Configuration for the ingest Lambda (environment variables only).

Nothing is hard-coded here that varies by deployment: bucket names and the
metrics namespace come from the Lambda environment (set by Terraform).
Constants that define the *contract* (prefixes, field lists, bounds) mirror
``src/pipeline_kit/config.py``; ``tests/test_mirrors.py`` asserts the two
agree.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# S3 layout
# ---------------------------------------------------------------------------

LANDING_PREFIX = "landing/orders/"
VALIDATED_PREFIX = "validated/orders/"
QUARANTINE_PREFIX = "quarantine/orders/"

METRICS_NAMESPACE_DEFAULT = "DataPipelineKit/Ingestion"

# ---------------------------------------------------------------------------
# Record contract (mirrors src/pipeline_kit/config.py)
# ---------------------------------------------------------------------------

REQUIRED_FIELDS: tuple[str, ...] = (
    "order_id",
    "order_ts",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "discount",
)

QUANTITY_MIN = 1
QUANTITY_MAX = 1000


@dataclass(frozen=True)
class LambdaConfig:
    """Validated Lambda configuration, built once per import from the env."""

    raw_bucket: str | None
    metrics_namespace: str

    @classmethod
    def from_env(cls, environ: dict[str, str] | None = None) -> "LambdaConfig":
        """Build from environment variables.

        ``RAW_BUCKET`` is optional here: the S3 event record normally carries
        the bucket name, and ``RAW_BUCKET`` is only the fallback. It is
        still an error (raised in ``app._source_bucket``) when neither
        source provides one.
        """
        env = os.environ if environ is None else environ
        return cls(
            raw_bucket=(env.get("RAW_BUCKET") or "").strip() or None,
            metrics_namespace=(
                env.get("METRICS_NAMESPACE") or METRICS_NAMESPACE_DEFAULT
            ).strip(),
        )


CONFIG = LambdaConfig.from_env()
