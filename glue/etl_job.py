"""
AWS Glue 4.0 (Spark 3.3 / Python 3.10) ETL job for the
"Production-Grade AWS Data Pipeline Starter Kit".

Thin driver only -- the stages live in the ``etl`` package next to this
file (``glue/src/etl/``):

    config.py   constants, Spark schemas, catalog DDL, argument parsing
    spark.py    Spark/Glue session bootstrap
    s3utils.py  S3 control-plane helpers (dt= prefixes, HEAD with retry)
    silver.py   bronze write + silver build/write (quarantine + dedupe)
    gold.py     dimensions (SCD2 / SCD1 / calendar) + fact_orders
    catalog.py  Glue Data Catalog registration
    quality.py  data-quality gates

Pipeline stages:
    1. Read validated orders JSONL + customer/product reference snapshots from S3 (raw).
    2. Bronze  : audit copy of validated orders as Parquet            -> bronze_orders
    3. Silver : typed, quarantined, deduplicated orders/customers/products
                 -> silver_orders / silver_customers / silver_products
    4. Gold   : dim_customer (SCD Type 2), dim_product (SCD Type 1),
                dim_date (calendar), fact_orders
    5. Register every output as an AWS Glue Data Catalog table.
    6. Run data-quality checks; commit the job.

Deployment: Terraform uploads this file as the job script and the ``etl``
package as ``--extra-py-files`` (see ``terraform/glue.tf``). Locally, the
``sys.path`` bootstrap below makes ``etl`` importable from ``glue/src``.

Everything (buckets, database, run date) comes from Glue job arguments --
no ARNs, account IDs, or bucket names are hard-coded.
"""

import os
import sys

# Make the `etl` package importable when run outside Glue (local tests,
# `spark-submit` from the repo). On Glue the `--extra-py-files` zip already
# puts `etl` on the path, so this is a no-op there.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from datetime import datetime  # noqa: E402

from awsglue.job import Job  # noqa: E402

from etl.catalog import register_table  # noqa: E402
from etl.config import (  # noqa: E402
    BRONZE_ORDERS_DDL,
    CUSTOMERS_DDL,
    DIM_CUSTOMER_DDL,
    DIM_DATE_DDL,
    DIM_PRODUCT_DDL,
    FACT_ORDERS_DDL,
    PRODUCTS_DDL,
    SILVER_ORDERS_DDL,
    parse_args,
    setup_logging,
)
from etl.gold import (  # noqa: E402
    build_dim_customer,
    build_dim_product,
    build_dim_date,
    build_fact_orders,
)
from etl.quality import run_data_quality_checks  # noqa: E402
from etl.silver import (  # noqa: E402
    build_silver_orders,
    build_silver_reference,
    read_reference_snapshot,
    read_validated_orders,
    write_bronze_orders,
    write_silver_orders,
    write_silver_reference,
)
from etl.spark import create_spark_session  # noqa: E402


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
