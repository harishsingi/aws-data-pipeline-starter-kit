"""Data-quality gates for the Glue job.

Two levels:

* :func:`check_orphan_rate` -- pure, Spark-free threshold check used by
  the fact build (and unit-tested via ``src/pipeline_kit/transforms.py``,
  which implements the same rule).
* :func:`run_data_quality_checks` -- hard gates over the built DataFrames.
  Any failure raises :class:`ValueError` and the driver exits non-zero
  without committing the job.
"""

from __future__ import annotations

from pyspark.sql.functions import col


def check_orphan_rate(orphan_rows: int, total_rows: int, threshold: float) -> None:
    """Fail fast when orphaned fact rows exceed ``threshold`` of the total.

    Raises:
        ValueError: If ``orphan_rows / total_rows`` is above ``threshold``.
    """
    if total_rows > 0 and orphan_rows / total_rows > threshold:
        raise ValueError(
            f"Orphaned fact rows {orphan_rows}/{total_rows} "
            f"({orphan_rows / total_rows:.2%}) exceed threshold "
            f"{threshold:.0%}; failing job."
        )


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
