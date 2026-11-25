"""Catalog registration and dimension loading for the Glue job."""

from __future__ import annotations

from etl.config import DIM_CUSTOMER_SCHEMA, DIM_PRODUCT_SCHEMA


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


def load_dim_customer(spark, database, log):
    """Load dim_customer, or an empty DF with the right schema on first run."""
    return load_dim_or_empty(spark, database, "dim_customer", DIM_CUSTOMER_SCHEMA, log)


def load_dim_product(spark, database, log):
    """Load dim_product, or an empty DF with the right schema on first run."""
    return load_dim_or_empty(spark, database, "dim_product", DIM_PRODUCT_SCHEMA, log)
