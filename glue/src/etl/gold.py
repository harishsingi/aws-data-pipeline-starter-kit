"""Gold layer: dimensions (SCD2 / SCD1 / calendar) and the fact table.

Surrogate keys are deterministic so re-runs produce stable keys:

* ``customer_sk`` = ``sha256_hex(customer_id || effective_from)``
  (see ``src/pipeline_kit/transforms.py`` for the testable equivalent)
* ``product_sk`` = ``sha256_hex(product_id)``
* ``date_sk`` = ``YYYYMMDD`` as an integer

Idempotency: dimensions are full overwrites and fact orders use dynamic
partition overwrite, so re-running for the same ``RUN_DATE`` replaces the
outputs instead of duplicating them.
"""

from __future__ import annotations

from datetime import date

from etl.catalog import load_dim_customer, load_dim_product
from etl.config import (
    DIM_CUSTOMER_COLUMNS,
    DIM_DATE_PARTITIONS,
    DIM_PRODUCT_COLUMNS,
    EFFECTIVE_TO_OPEN,
    ORPHAN_FAILURE_THRESHOLD,
    REFERENCE_PARTITIONS,
)
from etl.quality import check_orphan_rate

from pyspark.sql.functions import (
    broadcast,
    col,
    concat_ws,
    date_format,
    date_sub,
    explode,
    lit,
    sequence,
    sha2,
    to_date,
)


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
    existing = load_dim_customer(spark, database, log)
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
    merged.repartition(REFERENCE_PARTITIONS).write.mode("overwrite").parquet(location)
    return location, merged


def build_dim_product(spark, database, silver_products, marts_bucket, log):
    """SCD Type 1 upsert: overwrite changed attributes, insert new products.

    product_sk = sha2(product_id) is deterministic, so re-deriving it for
    incoming rows keeps keys stable across runs.
    """
    existing = load_dim_product(spark, database, log)

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
    merged.repartition(REFERENCE_PARTITIONS).write.mode("overwrite").parquet(location)
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
    dim_date.repartition(DIM_DATE_PARTITIONS).write.mode("overwrite").parquet(location)
    return location, dim_date


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
    check_orphan_rate(orphan_any, total, ORPHAN_FAILURE_THRESHOLD)

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
