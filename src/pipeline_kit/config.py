"""Configuration and constants for the pipeline kit.

This module is the single source of truth for every magic number and
naming convention that used to be scattered across the Lambda, the Glue job
and the sample-data generator. Deployable components that cannot import
this package (Lambda zip, Glue ``--extra-py-files`` zip) carry a mirrored
copy; ``tests/test_mirrors.py`` asserts the mirrors agree with these values.

No values here are secrets: bucket names come from the environment / job
arguments at runtime, never from this file.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import date, datetime

from pipeline_kit.errors import ConfigurationError

# ---------------------------------------------------------------------------
# Record contract (shared by Lambda validation, Glue schemas and dbt sources)
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

# Quantity: a small positive integer. The upper bound matches the Glue /
# Great Expectations contract (see ``dbt/models/sources.yml``).
QUANTITY_MIN = 1
QUANTITY_MAX = 1000

# Unit price: bounded well above any realistic product price in the
# synthetic catalog; the same bound is enforced by the GX suite.
UNIT_PRICE_MAX = 100_000

# ---------------------------------------------------------------------------
# S3 layout
# ---------------------------------------------------------------------------

LANDING_PREFIX = "landing/orders/"
VALIDATED_PREFIX = "validated/orders/"
QUARANTINE_PREFIX = "quarantine/orders/"
REFERENCE_PREFIX = "reference/"
BRONZE_PREFIX = "bronze/orders/"
SILVER_PREFIX = "silver/"
GOLD_PREFIX = "gold/"

METRICS_NAMESPACE_DEFAULT = "DataPipelineKit/Ingestion"

# ---------------------------------------------------------------------------
# Medallion / SCD behaviour
# ---------------------------------------------------------------------------

EFFECTIVE_TO_OPEN = "9999-12-31"  # open-ended SCD Type 2 effective_to
ORPHAN_FAILURE_THRESHOLD = 0.05  # fail the Glue job if >5% of fact rows are orphaned

# Spark repartitioning used by the Glue job. Small on purpose for a starter
# kit; raise them (via job arguments in future) if partitions grow.
DEFAULT_REPARTITION_PARTITIONS = 8
DEFAULT_REFERENCE_PARTITIONS = 4

# S3 bucket names follow the AWS naming rules; anything else is rejected
# before a job starts so a typo fails fast instead of mid-run.
_BUCKET_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9]$")


def parse_run_date(value: str) -> date:
    """Parse a ``YYYY-MM-DD`` run date, raising :class:`ConfigurationError`.

    Parameters:
        value: The run-date string, e.g. ``"2026-09-25"``.

    Raises:
        ConfigurationError: If the value is not a valid ``YYYY-MM-DD`` date.
    """
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        raise ConfigurationError(
            f"RUN_DATE must be YYYY-MM-DD, got {value!r}."
        ) from None


def validate_bucket_name(value: str, *, field: str) -> str:
    """Validate an S3 bucket name, returning it stripped.

    Raises:
        ConfigurationError: If the name is blank or not a valid S3 bucket name.
    """
    name = (value or "").strip()
    if not name:
        raise ConfigurationError(f"Job argument --{field} must not be blank.")
    if not _BUCKET_NAME_RE.match(name) or ".." in name:
        raise ConfigurationError(
            f"Job argument --{field} is not a valid S3 bucket name: {name!r}."
        )
    return name


@dataclass(frozen=True)
class PipelineConfig:
    """Validated pipeline configuration (buckets, database, run date).

    Built once at startup from job arguments or environment variables, then
    passed around instead of loose strings, so a bad bucket name or run date
    fails before any data is read.
    """

    raw_bucket: str
    curated_bucket: str
    marts_bucket: str
    database: str
    run_date: date

    @property
    def run_date_str(self) -> str:
        """The run date as ``YYYY-MM-DD`` (the ``dt=`` partition value)."""
        return self.run_date.isoformat()

    @classmethod
    def from_mapping(cls, mapping: dict[str, str]) -> "PipelineConfig":
        """Build from a ``{name: value}`` mapping (Glue job arguments).

        Raises:
            ConfigurationError: If a required key is missing/blank, a bucket
                name is invalid, or ``RUN_DATE`` is not ``YYYY-MM-DD``.
        """
        for key in ("RAW_BUCKET", "CURATED_BUCKET", "MARTS_BUCKET", "DATABASE"):
            if key not in mapping or not str(mapping[key]).strip():
                raise ConfigurationError(f"Job argument --{key} must not be blank.")
        return cls(
            raw_bucket=validate_bucket_name(mapping["RAW_BUCKET"], field="RAW_BUCKET"),
            curated_bucket=validate_bucket_name(mapping["CURATED_BUCKET"], field="CURATED_BUCKET"),
            marts_bucket=validate_bucket_name(mapping["MARTS_BUCKET"], field="MARTS_BUCKET"),
            database=str(mapping["DATABASE"]).strip(),
            run_date=parse_run_date(str(mapping.get("RUN_DATE", "")).strip()),
        )

    @classmethod
    def from_env(cls, environ: dict[str, str] | None = None) -> "PipelineConfig":
        """Build from environment variables (``RAW_BUCKET``, ...).

        ``RUN_DATE`` defaults to today (UTC) when unset, matching the
        sample-data generator's behaviour.
        """
        env = os.environ if environ is None else environ
        run_date_raw = (env.get("RUN_DATE") or "").strip()
        mapping = {
            "RAW_BUCKET": env.get("RAW_BUCKET", ""),
            "CURATED_BUCKET": env.get("CURATED_BUCKET", ""),
            "MARTS_BUCKET": env.get("MARTS_BUCKET", ""),
            "DATABASE": env.get("DATABASE", ""),
            "RUN_DATE": run_date_raw or datetime.now().date().isoformat(),
        }
        return cls.from_mapping(mapping)
