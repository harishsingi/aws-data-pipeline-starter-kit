"""Bronze and silver layers: readers, quarantine/dedupe, writers.

Idempotency: silver orders are written with dynamic partition overwrite,
so re-running the job for the same ``RUN_DATE`` replaces that partition
instead of duplicating it. Silver reference snapshots are full overwrites.
"""

from __future__ import annotations

from etl.config import (
    CUSTOMERS_SCHEMA,
    ORDERS_SCHEMA,
    PRODUCTS_SCHEMA,
    REFERENCE_PARTITIONS,
    SILVER_ORDERS_COLUMNS,
    WRITE_PARTITIONS,
)
from etl.s3utils import _s3_client, list_dt_prefixes, object_exists

from pyspark.sql.functions import col, lit, row_number, to_timestamp
from pyspark.sql.types import DecimalType, IntegerType
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------

def read_validated_orders(spark, raw_bucket, run_date_str, log):
    """Read the validated orders JSONL for RUN_DATE as raw strings/types.

    The upstream Lambda has already validated these rows; we keep every field
    for the bronze audit layer and cast/dedupe later in silver.

    Raises:
        ValueError: If no validated orders are found for the run date.
    """
    path = f"s3://{raw_bucket}/validated/orders/dt={run_date_str}/*.jsonl"
    log.info("Reading validated orders from %s", path)
    df = spark.read.schema(ORDERS_SCHEMA).json(path)
    count = df.count()
    log.info("Read %d validated order rows for dt=%s", count, run_date_str)
    if count == 0:
        raise ValueError(f"No validated orders found at {path}")
    return df


def read_reference_snapshot(spark, raw_bucket, run_date_str, kind, log):
    """Read a full reference snapshot (customers | products).

    Layout: s3://<RAW>/reference/<kind>/dt=<RUN_DATE>/<kind>.json.
    If the snapshot for RUN_DATE is missing, fall back to the latest
    available dt= prefix and log the substitution.

    Raises:
        ValueError: If ``kind`` is unknown, no snapshot exists at all, the
            latest snapshot is dated after RUN_DATE, or it is empty.
    """
    if kind not in ("customers", "products"):
        raise ValueError(f"Unknown reference kind {kind!r}.")
    schema = CUSTOMERS_SCHEMA if kind == "customers" else PRODUCTS_SCHEMA

    s3 = _s3_client()
    prefix = f"reference/{kind}/"
    target_key = f"{prefix}dt={run_date_str}/{kind}.json"

    if object_exists(s3, raw_bucket, target_key):
        chosen_dt = run_date_str
        log.info("Found %s snapshot for dt=%s", kind, run_date_str)
    else:
        log.warning(
            "No %s snapshot for dt=%s; looking for latest available dt prefix.",
            kind,
            run_date_str,
        )
        dts = list_dt_prefixes(s3, raw_bucket, prefix)
        if not dts:
            raise ValueError(
                f"No {kind} snapshots found under s3://{raw_bucket}/{prefix}"
            )
        chosen_dt = max(dts)  # YYYY-MM-DD strings sort chronologically
        if chosen_dt > run_date_str:
            raise ValueError(
                f"Latest available {kind} snapshot dt={chosen_dt} is AFTER "
                f"RUN_DATE {run_date_str}; refusing to use future data."
            )
        log.warning(
            "Falling back to %s snapshot dt=%s (requested dt=%s).",
            kind,
            chosen_dt,
            run_date_str,
        )

    path = f"s3://{raw_bucket}/{prefix}dt={chosen_dt}/{kind}.json"
    log.info("Reading %s snapshot from %s", kind, path)
    df = spark.read.schema(schema).json(path)
    count = df.count()
    log.info("Read %d %s rows from dt=%s", count, kind, chosen_dt)
    if count == 0:
        raise ValueError(f"Empty {kind} snapshot at {path}")
    return df


# ---------------------------------------------------------------------------
# Bronze layer
# ---------------------------------------------------------------------------

def write_bronze_orders(orders_raw, raw_bucket, run_date_str, log):
    """Write the raw validated orders as Parquet (audit layer)."""
    location = f"s3://{raw_bucket}/bronze/orders/dt={run_date_str}/"
    log.info("Writing bronze orders to %s", location)
    (
        orders_raw.repartition(WRITE_PARTITIONS)
        .write.mode("overwrite")
        .parquet(location)
    )
    count = orders_raw.count()
    log.info("Bronze orders written: %d rows -> %s", count, location)
    return f"s3://{raw_bucket}/bronze/orders/"  # base dir, partition subdir excluded


# ---------------------------------------------------------------------------
# Silver layer
# ---------------------------------------------------------------------------

def build_silver_orders(orders_raw, run_date_str, log):
    """Cast, quarantine, and dedupe orders for the silver layer.

    - order_ts: ISO8601 string -> timestamp
    - quantity/unit_price/discount cast to int/decimal(10,2)
    - rows with null order_id/customer_id/product_id are quarantined
      (counted and dropped, never silently fixed)
    - dedupe by order_id keeping the latest order_ts

    Raises:
        ValueError: If silver orders is empty after quarantine/dedupe.
    """
    typed = (
        orders_raw.withColumn("order_ts", to_timestamp(col("order_ts")))
        .withColumn("quantity", col("quantity").cast(IntegerType()))
        .withColumn("unit_price", col("unit_price").cast(DecimalType(10, 2)))
        .withColumn("discount", col("discount").cast(DecimalType(10, 2)))
    )

    quarantined = typed.filter(
        col("order_id").isNull()
        | col("customer_id").isNull()
        | col("product_id").isNull()
    )
    quarantine_count = quarantined.count()
    if quarantine_count:
        log.warning(
            "Quarantined %d order rows with null order_id/customer_id/product_id "
            "(excluded from silver).",
            quarantine_count,
        )
    clean = typed.filter(
        col("order_id").isNotNull()
        & col("customer_id").isNotNull()
        & col("product_id").isNotNull()
    )

    window = Window.partitionBy("order_id").orderBy(col("order_ts").desc_nulls_last())
    deduped = (
        clean.withColumn("_rn", row_number().over(window))
        .filter(col("_rn") == 1)
        .drop("_rn")
        .withColumn("dt", lit(run_date_str))
        .select(*SILVER_ORDERS_COLUMNS)
    )

    in_count = typed.count()
    out_count = deduped.count()
    log.info(
        "Silver orders: %d validated -> %d clean (quarantined=%d, dupes_removed=%d)",
        in_count,
        out_count,
        quarantine_count,
        in_count - quarantine_count - out_count,
    )
    if out_count == 0:
        raise ValueError("Silver orders is empty after quarantine/dedupe.")
    return deduped


def write_silver_orders(silver_orders, curated_bucket, run_date_str, log):
    """Write silver orders partitioned by dt (dynamic overwrite)."""
    location = f"s3://{curated_bucket}/silver/orders/dt={run_date_str}/"
    log.info("Writing silver orders to %s", location)
    (
        silver_orders.repartition(WRITE_PARTITIONS, "dt")
        .write.mode("overwrite")
        .partitionBy("dt")
        .parquet(f"s3://{curated_bucket}/silver/orders/")
    )
    log.info("Silver orders written to %s", location)
    return f"s3://{curated_bucket}/silver/orders/"


def build_silver_reference(ref_df, id_col, run_date_str, log):
    """Dedupe a full reference snapshot by id keeping the latest updated_ts."""
    window = Window.partitionBy(id_col).orderBy(col("updated_ts").desc_nulls_last())
    deduped = (
        ref_df.withColumn("_rn", row_number().over(window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )
    in_count = ref_df.count()
    out_count = deduped.count()
    log.info(
        "Silver %s: %d snapshot rows -> %d unique (dupes_removed=%d)",
        id_col,
        in_count,
        out_count,
        in_count - out_count,
    )
    return deduped


def write_silver_reference(silver_df, curated_bucket, kind, log):
    """Full-overwrite write of a silver reference snapshot (not partitioned)."""
    base = f"s3://{curated_bucket}/silver/{kind}/"
    log.info("Writing silver %s (full snapshot) to %s", kind, base)
    silver_df.repartition(REFERENCE_PARTITIONS).write.mode("overwrite").parquet(base)
    log.info("Silver %s written: %d rows", kind, silver_df.count())
    return base
