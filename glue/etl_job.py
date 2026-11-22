"""
AWS Glue 4.0 (Spark 3.3 / Python 3.10) ETL job for the
"Production-Grade AWS Data Pipeline Starter Kit".

Pipeline stages:
    1. Read validated orders JSONL + customer/product reference snapshots from S3 (raw).
    2. Bronze  : audit copy of validated orders as Parquet            -> bronze_orders
    3. Silver : typed, quarantined, deduplicated orders/customers/products
                 -> silver_orders / silver_customers / silver_products
    4. Gold   : dim_customer (SCD Type 2), dim_product (SCD Type 1),
                dim_date (calendar), fact_orders
    5. Register every output as an AWS Glue Data Catalog table.
    6. Run data-quality checks; commit the job.

Everything (buckets, database, run date) comes from Glue job arguments --
no ARNs, account IDs, or bucket names are hard-coded.
"""

import logging
import sys
from datetime import datetime, date

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    broadcast,
    col,
    concat_ws,
    date_format,
    date_sub,
    explode,
    lit,
    row_number,
    sequence,
    sha2,
    to_date,
    to_timestamp,
)
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
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EFFECTIVE_TO_OPEN = "9999-12-31"          # open-ended SCD2 effective_to
ORPHAN_FAILURE_THRESHOLD = 0.05          # fail job if >5% fact rows are orphaned

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
# Argument parsing + logging
# ---------------------------------------------------------------------------

def parse_args(argv):
    """Parse Glue job arguments and validate them.

    Raises:
        ValueError: if RUN_DATE is not in YYYY-MM-DD format or any required
            argument is blank.
    """
    args = getResolvedOptions(
        argv,
        [
            "JOB_NAME",
            "RAW_BUCKET",
            "CURATED_BUCKET",
            "MARTS_BUCKET",
            "DATABASE",
            "RUN_DATE",
        ],
    )

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


# ---------------------------------------------------------------------------
# Spark / Glue bootstrap
# ---------------------------------------------------------------------------

def create_spark_session():
    """Build the SparkSession via GlueContext with sane pipeline defaults."""
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    # Overwriting a single dt= partition must not wipe sibling partitions.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    # Parquet timestamps stay readable by Athena/Redshift Spectrum.
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS")
    return glue_context, spark


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

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


def object_exists(s3_client, bucket, key):
    """True if the S3 object exists (HEAD request)."""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------

def read_validated_orders(spark, raw_bucket, run_date_str, log):
    """Read the validated orders JSONL for RUN_DATE as raw strings/types.

    The upstream Lambda has already validated these rows; we keep every field
    for the bronze audit layer and cast/dedupe later in silver.
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
    """
    if kind not in ("customers", "products"):
        raise ValueError(f"Unknown reference kind {kind!r}.")
    schema = CUSTOMERS_SCHEMA if kind == "customers" else PRODUCTS_SCHEMA

    s3 = boto3.client("s3")
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
        orders_raw.repartition(8)
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
        silver_orders.repartition(8, "dt")
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
    silver_df.repartition(4).write.mode("overwrite").parquet(base)
    log.info("Silver %s written: %d rows", kind, silver_df.count())
    return base


# ---------------------------------------------------------------------------
# Catalog registration
# ---------------------------------------------------------------------------

def register_table(spark, database, table, location, columns_ddl, partition_cols, log):
    """CREATE TABLE IF NOT EXISTS ... USING PARQUET, then MSCK REPAIR TABLE.

    columns_ddl: list of (name, spark_sql_type) tuples, e.g.
        [("order_id", "STRING"), ("dt", "STRING")].
    partition_cols: list of column names, e.g. ["dt"] (empty list if none).
    """
    cols_sql = ", ".join(f"`{name}` {dtype}" for name, dtype in columns_ddl)
    ddl = (
        f"CREATE TABLE IF NOT EXISTS `{database}`.`{table}` ({cols_sql}) "
        f"USING PARQUET"
    )
    if partition_cols:
        parts_sql = ", ".join(f"`{p}` STRING" for p in partition_cols)
        ddl += f" PARTITIONED BY ({parts_sql})"
    ddl += f" LOCATION '{location}'"
    log.info("Registering table %s.%s at %s", database, table, location)
    spark.sql(ddl)

    if partition_cols:
        # MSCK picks up dt= partitions written by Spark. It can fail on
        # transient S3 listing issues; that must not fail the pipeline.
        try:
            spark.sql(f"MSCK REPAIR TABLE `{database}`.`{table}`")
            log.info("MSCK REPAIR TABLE completed for %s.%s", database, table)
        except Exception as exc:  # noqa: BLE001 - repair is best-effort
            log.warning(
                "MSCK REPAIR TABLE failed for %s.%s (continuing): %s",
                database,
                table,
                exc,
            )


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


# ---------------------------------------------------------------------------
# Gold layer: dimensions
# ---------------------------------------------------------------------------

def load_dim_or_empty(spark, database, table, schema, log):
    """Load an existing dimension table; return an empty DF if it is missing."""
    qualified = f"{database}.{table}"
    try:
        df = spark.table(qualified)
        count = df.count()
        log.info("Loaded existing %s: %d rows", qualified, count)
        return df
    except Exception as exc:  # table not registered yet -> first run
        log.info(
            "Dimension %s not found (%s); treating as first run.",
            qualified,
            type(exc).__name__,
        )
        return spark.createDataFrame([], schema)


def new_customer_sk(df):
    """Deterministic surrogate key: sha2(customer_id || effective_from)."""
    return df.withColumn(
        "customer_sk",
        sha2(concat_ws("||", col("customer_id"), col("effective_from").cast("string")), 256),
    )


def build_dim_customer(spark, database, silver_customers, run_date, marts_bucket, log):
    """SCD Type 2 full-refresh merge for customers.

    - full outer join incoming snapshot vs current dim rows (is_current=true)
    - changed (full_name/email/segment) -> expire old version
      (effective_to = RUN_DATE - 1 day, is_current=false) + insert new version
    - unchanged -> carry the current row over untouched
    - brand-new customers -> insert as new current versions
    - customers absent from the snapshot are carried over unchanged (no
      deletion signal is inferred from a missing snapshot row)
    """
    run_date_str = run_date.isoformat()
    existing = load_dim_or_empty(spark, database, "dim_customer", DIM_CUSTOMER_SCHEMA, log)
    current = existing.filter(col("is_current") == True)  # noqa: E712

    incoming = (
        silver_customers.select("customer_id", "full_name", "email", "segment")
        .distinct()
    )

    # Explicit join condition (not on="customer_id") so both qualified key
    # columns survive the full outer join for the null checks below.
    joined = incoming.alias("inc").join(
        current.alias("cur"),
        col("inc.customer_id") == col("cur.customer_id"),
        how="full_outer",
    )

    inc_present = col("inc.customer_id").isNotNull()
    cur_present = col("cur.customer_id").isNotNull()
    changed = (
        ~col("inc.full_name").eqNullSafe(col("cur.full_name"))
        | ~col("inc.email").eqNullSafe(col("cur.email"))
        | ~col("inc.segment").eqNullSafe(col("cur.segment"))
    )

    # 1) Expired versions: previously current rows whose attributes changed.
    expired = (
        joined.filter(cur_present & inc_present & changed)
        .select(
            col("cur.customer_sk").alias("customer_sk"),
            col("cur.customer_id").alias("customer_id"),
            col("cur.full_name").alias("full_name"),
            col("cur.email").alias("email"),
            col("cur.segment").alias("segment"),
            col("cur.effective_from").alias("effective_from"),
            date_sub(lit(run_date_str).cast("date"), 1).alias("effective_to"),
            lit(False).alias("is_current"),
        )
    )

    # 2) New versions: brand-new customers + changed customers.
    new_versions = (
        joined.filter(inc_present & (~cur_present | changed))
        .select(
            col("inc.customer_id").alias("customer_id"),
            col("inc.full_name").alias("full_name"),
            col("inc.email").alias("email"),
            col("inc.segment").alias("segment"),
            lit(run_date_str).cast("date").alias("effective_from"),
            lit(EFFECTIVE_TO_OPEN).cast("date").alias("effective_to"),
            lit(True).alias("is_current"),
        )
    )
    new_versions = new_customer_sk(new_versions).select(*DIM_CUSTOMER_COLUMNS)

    # 3) Carried over: unchanged current rows + rows absent from the snapshot.
    carried = (
        joined.filter(cur_present & (~inc_present | ~changed))
        .select(
            col("cur.customer_sk").alias("customer_sk"),
            col("cur.customer_id").alias("customer_id"),
            col("cur.full_name").alias("full_name"),
            col("cur.email").alias("email"),
            col("cur.segment").alias("segment"),
            col("cur.effective_from").alias("effective_from"),
            col("cur.effective_to").alias("effective_to"),
            col("cur.is_current").alias("is_current"),
        )
    )

    # 4) Historical rows that were already expired stay as-is.
    history = existing.filter(col("is_current") == False)  # noqa: E712

    merged = (
        expired.unionByName(carried)
        .unionByName(new_versions)
        .unionByName(history)
        .select(*DIM_CUSTOMER_COLUMNS)
    )

    expired_count = expired.count()
    new_count = new_versions.count()
    total_count = merged.count()
    log.info(
        "dim_customer merge: expired=%d, new_versions=%d, total=%d",
        expired_count,
        new_count,
        total_count,
    )

    location = f"s3://{marts_bucket}/gold/dim_customer/"
    log.info("Writing dim_customer (full overwrite) to %s", location)
    merged.repartition(4).write.mode("overwrite").parquet(location)
    return location, merged


def build_dim_product(spark, database, silver_products, marts_bucket, log):
    """SCD Type 1 upsert: overwrite changed attributes, insert new products.

    product_sk = sha2(product_id) is deterministic, so re-deriving it for
    incoming rows keeps keys stable across runs.
    """
    existing = load_dim_or_empty(spark, database, "dim_product", DIM_PRODUCT_SCHEMA, log)

    incoming = (
        silver_products.select("product_id", "product_name", "category", "unit_price")
        .distinct()
        .withColumn("product_sk", sha2(col("product_id"), 256))
        .select(*DIM_PRODUCT_COLUMNS)
    )

    existing_only = existing.join(
        incoming.select("product_id"), on="product_id", how="left_anti"
    )

    merged = incoming.unionByName(existing_only).select(*DIM_PRODUCT_COLUMNS)

    location = f"s3://{marts_bucket}/gold/dim_product/"
    log.info("Writing dim_product (full overwrite) to %s", location)
    merged.repartition(4).write.mode("overwrite").parquet(location)
    log.info(
        "dim_product: incoming=%d, carried=%d, total=%d",
        incoming.count(),
        existing_only.count(),
        merged.count(),
    )
    return location, merged


def build_dim_date(spark, run_date, marts_bucket, log):
    """Generate a full calendar year of dates for the RUN_DATE year.

    A whole year is a few thousand rows -- negligible cost -- and it makes the
    dimension useful for any order_ts that falls near RUN_DATE, not just on it.
    """
    year = run_date.year
    start = date(year, 1, 1).isoformat()
    end = date(year, 12, 31).isoformat()
    log.info("Generating dim_date calendar for %d (%s to %s)", year, start, end)

    dates = spark.sql(
        f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), "
        f"interval 1 day)) AS full_date"
    )
    dim_date = (
        dates.withColumn("year", date_format(col("full_date"), "yyyy").cast("int"))
        .withColumn("month", date_format(col("full_date"), "M").cast("int"))
        .withColumn("day", date_format(col("full_date"), "d").cast("int"))
        # integer division: months 1-3 -> 1, 4-6 -> 2, ...
        .withColumn("quarter", (((col("month") - 1) / 3).cast("int") + 1))
        .withColumn(
            "date_sk",
            col("year") * 10000 + col("month") * 100 + col("day"),
        )
        .withColumn("day_of_week", date_format(col("full_date"), "EEEE"))
        .select("date_sk", "full_date", "year", "quarter", "month", "day", "day_of_week")
    )

    count = dim_date.count()
    location = f"s3://{marts_bucket}/gold/dim_date/"
    log.info("Writing dim_date (%d rows, full overwrite) to %s", count, location)
    dim_date.repartition(1).write.mode("overwrite").parquet(location)
    return location, dim_date


# ---------------------------------------------------------------------------
# Gold layer: fact
# ---------------------------------------------------------------------------

def build_fact_orders(spark, database, silver_orders, dim_customer, dim_product,
                      dim_date, run_date_str, marts_bucket, log):
    """Build fact_orders by resolving surrogate keys from the dimensions.

    - customer: join to the CURRENT dim_customer row (broadcast)
    - product: join to dim_product (broadcast)
    - date: join dim_date on date(order_ts)
    Rows with missing dimension matches are dropped, but orphan counts are
    logged as data-quality warnings -- and the job FAILS if more than 5% of
    fact rows are orphaned.
    """
    dim_cust_current = dim_customer.filter(col("is_current") == True)  # noqa: E712

    enriched = (
        silver_orders.alias("o")
        .join(broadcast(dim_cust_current).alias("c"), on="customer_id", how="left")
        .join(broadcast(dim_product).alias("p"), on="product_id", how="left")
        .join(dim_date.alias("d"), to_date(col("o.order_ts")) == col("d.full_date"), how="left")
    )

    total = enriched.count()
    orphan_customer = enriched.filter(col("c.customer_sk").isNull()).count()
    orphan_product = enriched.filter(col("p.product_sk").isNull()).count()
    orphan_date = enriched.filter(col("d.date_sk").isNull()).count()
    orphan_any = enriched.filter(
        col("c.customer_sk").isNull()
        | col("p.product_sk").isNull()
        | col("d.date_sk").isNull()
    ).count()

    log.warning(
        "Orphan fact rows: customer=%d, product=%d, date=%d, any=%d of %d total",
        orphan_customer,
        orphan_product,
        orphan_date,
        orphan_any,
        total,
    )
    if total > 0 and orphan_any / total > ORPHAN_FAILURE_THRESHOLD:
        raise ValueError(
            f"Orphaned fact rows {orphan_any}/{total} "
            f"({orphan_any / total:.2%}) exceed threshold "
            f"{ORPHAN_FAILURE_THRESHOLD:.0%}; failing job."
        )

    fact = (
        enriched.filter(
            col("c.customer_sk").isNotNull()
            & col("p.product_sk").isNotNull()
            & col("d.date_sk").isNotNull()
        )
        .withColumn(
            "line_total",
            (col("o.quantity") * (col("o.unit_price") - col("o.discount"))).cast(
                "decimal(12,2)"
            ),
        )
        .select(
            col("o.order_id").alias("order_id"),
            col("o.order_ts").alias("order_ts"),
            col("d.date_sk").alias("date_sk"),
            col("c.customer_sk").alias("customer_sk"),
            col("p.product_sk").alias("product_sk"),
            col("o.quantity").alias("quantity"),
            col("o.unit_price").alias("unit_price"),
            col("o.discount").alias("discount"),
            col("line_total"),
            lit(run_date_str).alias("dt"),
        )
    )

    fact_count = fact.count()
    log.info("fact_orders: %d rows (dropped %d orphans)", fact_count, orphan_any)

    base = f"s3://{marts_bucket}/gold/fact_orders/"
    log.info("Writing fact_orders (dt=%s, dynamic overwrite) to %s", run_date_str, base)
    (
        fact.repartition(8, "dt")
        .write.mode("overwrite")
        .partitionBy("dt")
        .parquet(base)
    )
    return base, fact


# ---------------------------------------------------------------------------
# Data quality checks
# ---------------------------------------------------------------------------

def run_data_quality_checks(silver_orders, fact_orders, dim_customer, dim_product, log):
    """Hard data-quality gates. Raises ValueError on any failure."""
    log.info("Running data-quality checks...")

    silver_count = silver_orders.count()
    if silver_count <= 0:
        raise ValueError("DQ FAILED: silver_orders row count is 0.")
    log.info("DQ OK: silver_orders row count = %d", silver_count)

    fact_count = fact_orders.count()
    if fact_count <= 0:
        raise ValueError("DQ FAILED: fact_orders row count is 0.")
    log.info("DQ OK: fact_orders row count = %d", fact_count)

    null_cust_sk = dim_customer.filter(
        (col("is_current") == True) & col("customer_sk").isNull()  # noqa: E712
    ).count()
    if null_cust_sk:
        raise ValueError(f"DQ FAILED: {null_cust_sk} current dim_customer rows lack customer_sk.")
    log.info("DQ OK: no null customer_sk in current dim_customer rows.")

    null_prod_sk = dim_product.filter(col("product_sk").isNull()).count()
    if null_prod_sk:
        raise ValueError(f"DQ FAILED: {null_prod_sk} dim_product rows lack product_sk.")
    log.info("DQ OK: no null product_sk in dim_product.")

    for sk_col in ("customer_sk", "product_sk", "date_sk"):
        nulls = fact_orders.filter(col(sk_col).isNull()).count()
        if nulls:
            raise ValueError(f"DQ FAILED: {nulls} fact_orders rows lack {sk_col}.")
    log.info("DQ OK: no null surrogate keys in fact_orders.")

    negative = fact_orders.filter(col("line_total") < 0).count()
    if negative:
        raise ValueError(f"DQ FAILED: {negative} fact_orders rows have line_total < 0.")
    log.info("DQ OK: all line_total values >= 0.")

    log.info("All data-quality checks passed.")


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def run_stage(log, timings, name, fn, *args, **kwargs):
    """Run a pipeline stage, logging its wall-clock time."""
    log.info("=== STAGE START: %s ===", name)
    start = datetime.now()
    result = fn(*args, **kwargs)
    elapsed = (datetime.now() - start).total_seconds()
    timings[name] = elapsed
    log.info("=== STAGE DONE: %s (%.1fs) ===", name, elapsed)
    return result


def main():
    log = setup_logging()

    args, run_date = parse_args(sys.argv)
    run_date_str = run_date.isoformat()
    raw_bucket = args["RAW_BUCKET"]
    curated_bucket = args["CURATED_BUCKET"]
    marts_bucket = args["MARTS_BUCKET"]
    database = args["DATABASE"]

    log.info(
        "Starting ETL job %s: RUN_DATE=%s DATABASE=%s",
        args["JOB_NAME"],
        run_date_str,
        database,
    )
    log.info(
        "Buckets: raw=%s curated=%s marts=%s",
        raw_bucket,
        curated_bucket,
        marts_bucket,
    )

    glue_context, spark = create_spark_session()
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    timings = {}
    try:
        # ---- Read inputs ------------------------------------------------
        orders_raw = run_stage(
            log, timings, "read_validated_orders",
            read_validated_orders, spark, raw_bucket, run_date_str, log,
        )
        customers_raw = run_stage(
            log, timings, "read_customers_snapshot",
            read_reference_snapshot, spark, raw_bucket, run_date_str, "customers", log,
        )
        products_raw = run_stage(
            log, timings, "read_products_snapshot",
            read_reference_snapshot, spark, raw_bucket, run_date_str, "products", log,
        )

        # ---- Bronze -----------------------------------------------------
        bronze_base = run_stage(
            log, timings, "write_bronze_orders",
            write_bronze_orders, orders_raw, raw_bucket, run_date_str, log,
        )
        run_stage(
            log, timings, "register_bronze_orders",
            register_table, spark, database, "bronze_orders",
            bronze_base, BRONZE_ORDERS_DDL, ["dt"], log,
        )

        # ---- Silver -----------------------------------------------------
        silver_orders = run_stage(
            log, timings, "build_silver_orders",
            build_silver_orders, orders_raw, run_date_str, log,
        )
        silver_orders_base = run_stage(
            log, timings, "write_silver_orders",
            write_silver_orders, silver_orders, curated_bucket, run_date_str, log,
        )
        run_stage(
            log, timings, "register_silver_orders",
            register_table, spark, database, "silver_orders",
            silver_orders_base, SILVER_ORDERS_DDL, ["dt"], log,
        )

        silver_customers = run_stage(
            log, timings, "build_silver_customers",
            build_silver_reference, customers_raw, "customer_id", run_date_str, log,
        )
        customers_base = run_stage(
            log, timings, "write_silver_customers",
            write_silver_reference, silver_customers, curated_bucket, "customers", log,
        )
        run_stage(
            log, timings, "register_silver_customers",
            register_table, spark, database, "silver_customers",
            customers_base, CUSTOMERS_DDL, [], log,
        )

        silver_products = run_stage(
            log, timings, "build_silver_products",
            build_silver_reference, products_raw, "product_id", run_date_str, log,
        )
        products_base = run_stage(
            log, timings, "write_silver_products",
            write_silver_reference, silver_products, curated_bucket, "products", log,
        )
        run_stage(
            log, timings, "register_silver_products",
            register_table, spark, database, "silver_products",
            products_base, PRODUCTS_DDL, [], log,
        )

        # ---- Gold: dimensions -------------------------------------------
        dim_customer_base, dim_customer = run_stage(
            log, timings, "build_dim_customer",
            build_dim_customer, spark, database, silver_customers,
            run_date, marts_bucket, log,
        )
        run_stage(
            log, timings, "register_dim_customer",
            register_table, spark, database, "dim_customer",
            dim_customer_base, DIM_CUSTOMER_DDL, [], log,
        )

        dim_product_base, dim_product = run_stage(
            log, timings, "build_dim_product",
            build_dim_product, spark, database, silver_products, marts_bucket, log,
        )
        run_stage(
            log, timings, "register_dim_product",
            register_table, spark, database, "dim_product",
            dim_product_base, DIM_PRODUCT_DDL, [], log,
        )

        dim_date_base, dim_date = run_stage(
            log, timings, "build_dim_date",
            build_dim_date, spark, run_date, marts_bucket, log,
        )
        run_stage(
            log, timings, "register_dim_date",
            register_table, spark, database, "dim_date",
            dim_date_base, DIM_DATE_DDL, [], log,
        )

        # ---- Gold: fact --------------------------------------------------
        fact_base, fact_orders = run_stage(
            log, timings, "build_fact_orders",
            build_fact_orders, spark, database, silver_orders, dim_customer,
            dim_product, dim_date, run_date_str, marts_bucket, log,
        )
        run_stage(
            log, timings, "register_fact_orders",
            register_table, spark, database, "fact_orders",
            fact_base, FACT_ORDERS_DDL, ["dt"], log,
        )

        # ---- Data quality ------------------------------------------------
        run_stage(
            log, timings, "data_quality_checks",
            run_data_quality_checks,
            silver_orders, fact_orders, dim_customer, dim_product, log,
        )

        log.info("Pipeline stage timings (seconds):")
        for name, seconds in timings.items():
            log.info("  %-28s %8.1f", name, seconds)

        log.info("ETL job %s SUCCEEDED for RUN_DATE=%s", args["JOB_NAME"], run_date_str)
        job.commit()
    except Exception:
        log.exception(
            "ETL job %s FAILED for RUN_DATE=%s", args["JOB_NAME"], run_date_str
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
