"""Glue job configuration: constants, schemas, DDL and argument parsing.

The schemas need ``pyspark`` types (they are Spark schemas); everything
else here -- constants, ``parse_args``, ``setup_logging`` -- is stdlib-only.
Unit tests stub ``pyspark`` in ``sys.modules`` before importing this module
(see ``tests/test_glue_modules.py``). The constant values mirror
``src/pipeline_kit/config.py``; ``tests/test_mirrors.py`` asserts the two
agree.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, date

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EFFECTIVE_TO_OPEN = "9999-12-31"          # open-ended SCD2 effective_to
ORPHAN_FAILURE_THRESHOLD = 0.05          # fail job if >5% fact rows are orphaned

# Spark repartitioning for writes. Small on purpose for a starter kit.
WRITE_PARTITIONS = 8
REFERENCE_PARTITIONS = 4
DIM_DATE_PARTITIONS = 1

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("order_ts", StringType(), True),   # ISO8601, cast to timestamp later
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DecimalType(10, 2), True),
        StructField("discount", DecimalType(10, 2), True),
    ]
)

CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("updated_ts", TimestampType(), True),
    ]
)

PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("unit_price", DecimalType(10, 2), True),
        StructField("updated_ts", TimestampType(), True),
    ]
)

SILVER_ORDERS_COLUMNS = [
    "order_id",
    "order_ts",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "discount",
    "dt",
]

DIM_CUSTOMER_COLUMNS = [
    "customer_sk",
    "customer_id",
    "full_name",
    "email",
    "segment",
    "effective_from",
    "effective_to",
    "is_current",
]

DIM_CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_sk", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("full_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("effective_from", DateType(), False),
        StructField("effective_to", DateType(), False),
        StructField("is_current", BooleanType(), False),
    ]
)

DIM_PRODUCT_COLUMNS = [
    "product_sk",
    "product_id",
    "product_name",
    "category",
    "unit_price",
]

DIM_PRODUCT_SCHEMA = StructType(
    [
        StructField("product_sk", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("unit_price", DecimalType(10, 2), True),
    ]
)

# ---------------------------------------------------------------------------
# Catalog DDL (column name, Spark SQL type) per table
# ---------------------------------------------------------------------------

SILVER_ORDERS_DDL = [
    ("order_id", "STRING"),
    ("order_ts", "TIMESTAMP"),
    ("customer_id", "STRING"),
    ("product_id", "STRING"),
    ("quantity", "INT"),
    ("unit_price", "DECIMAL(10,2)"),
    ("discount", "DECIMAL(10,2)"),
    ("dt", "STRING"),
]

CUSTOMERS_DDL = [
    ("customer_id", "STRING"),
    ("full_name", "STRING"),
    ("email", "STRING"),
    ("segment", "STRING"),
    ("updated_ts", "TIMESTAMP"),
]

PRODUCTS_DDL = [
    ("product_id", "STRING"),
    ("product_name", "STRING"),
    ("category", "STRING"),
    ("unit_price", "DECIMAL(10,2)"),
    ("updated_ts", "TIMESTAMP"),
]

DIM_CUSTOMER_DDL = [
    ("customer_sk", "STRING"),
    ("customer_id", "STRING"),
    ("full_name", "STRING"),
    ("email", "STRING"),
    ("segment", "STRING"),
    ("effective_from", "DATE"),
    ("effective_to", "DATE"),
    ("is_current", "BOOLEAN"),
]

DIM_PRODUCT_DDL = [
    ("product_sk", "STRING"),
    ("product_id", "STRING"),
    ("product_name", "STRING"),
    ("category", "STRING"),
    ("unit_price", "DECIMAL(10,2)"),
]

DIM_DATE_DDL = [
    ("date_sk", "INT"),
    ("full_date", "DATE"),
    ("year", "INT"),
    ("quarter", "INT"),
    ("month", "INT"),
    ("day", "INT"),
    ("day_of_week", "STRING"),
]

FACT_ORDERS_DDL = [
    ("order_id", "STRING"),
    ("order_ts", "TIMESTAMP"),
    ("date_sk", "INT"),
    ("customer_sk", "STRING"),
    ("product_sk", "STRING"),
    ("quantity", "INT"),
    ("unit_price", "DECIMAL(10,2)"),
    ("discount", "DECIMAL(10,2)"),
    ("line_total", "DECIMAL(12,2)"),
    ("dt", "STRING"),
]

# Bronze orders carry the same columns as silver orders; dt is the partition
# column, so it is excluded from the CREATE TABLE column list.
BRONZE_ORDERS_DDL = SILVER_ORDERS_DDL[:-1]

# Glue job argument names (kept in one place so the driver and Terraform
# cannot drift apart silently).
REQUIRED_JOB_ARGS = [
    "JOB_NAME",
    "RAW_BUCKET",
    "CURATED_BUCKET",
    "MARTS_BUCKET",
    "DATABASE",
    "RUN_DATE",
]


# ---------------------------------------------------------------------------
# Argument parsing + logging
# ---------------------------------------------------------------------------

def parse_args(argv):
    """Parse Glue job arguments and validate them.

    Returns ``(args, run_date)`` where ``args`` is the dict returned by
    ``getResolvedOptions`` and ``run_date`` is a ``datetime.date``.

    Raises:
        ValueError: if RUN_DATE is not in YYYY-MM-DD format or any required
            argument is blank.
    """
    # Imported lazily: awsglue exists only on Glue, and this module must
    # stay importable (for tests) without it.
    from awsglue.utils import getResolvedOptions

    args = getResolvedOptions(argv, REQUIRED_JOB_ARGS)

    for key in ("RAW_BUCKET", "CURATED_BUCKET", "MARTS_BUCKET", "DATABASE"):
        if not args[key] or not args[key].strip():
            raise ValueError(f"Job argument --{key} must not be blank.")

    try:
        run_date = datetime.strptime(args["RUN_DATE"], "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(
            f"Job argument --RUN_DATE must be YYYY-MM-DD, got {args['RUN_DATE']!r}."
        )

    return args, run_date


def setup_logging():
    """Configure root logging at INFO level for the driver."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        stream=sys.stdout,
        force=True,
    )
    # Keep noisy AWS SDK retry chatter out of the logs.
    logging.getLogger("botocore").setLevel(logging.WARNING)
    return logging.getLogger("etl_job")
