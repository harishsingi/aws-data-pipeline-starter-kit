"""Shared pure-Python package for the AWS Data Pipeline Starter Kit.

Stdlib only. Nothing in here imports ``boto3``, ``pyspark``, ``pandas`` or
Great Expectations, so it can be unit-tested anywhere with no AWS account,
no Spark cluster and no third-party packages installed.

The deployable components (the Lambda in ``lambda/src`` and the Glue job in
``glue/src/etl``) are packaged as self-contained zips, so they carry small
mirrored copies of the pieces they need instead of importing this package.
Where a mirror exists the module header says so, and
``tests/test_mirrors.py`` asserts the mirrors agree with this package on a
shared corpus.
"""

__version__ = "0.2.0"

from pipeline_kit.config import (  # noqa: F401
    EFFECTIVE_TO_OPEN,
    LANDING_PREFIX,
    ORPHAN_FAILURE_THRESHOLD,
    QUANTITY_MAX,
    QUANTITY_MIN,
    QUARANTINE_PREFIX,
    REQUIRED_FIELDS,
    UNIT_PRICE_MAX,
    VALIDATED_PREFIX,
    PipelineConfig,
)
from pipeline_kit.errors import (  # noqa: F401
    ConfigurationError,
    DataQualityError,
    PipelineError,
    RetryExhaustedError,
    UpstreamDataError,
    ValidationError,
)

__all__ = [
    "EFFECTIVE_TO_OPEN",
    "LANDING_PREFIX",
    "ORPHAN_FAILURE_THRESHOLD",
    "QUANTITY_MAX",
    "QUANTITY_MIN",
    "QUARANTINE_PREFIX",
    "REQUIRED_FIELDS",
    "UNIT_PRICE_MAX",
    "VALIDATED_PREFIX",
    "ConfigurationError",
    "DataQualityError",
    "PipelineConfig",
    "PipelineError",
    "RetryExhaustedError",
    "UpstreamDataError",
    "ValidationError",
    "__version__",
]
