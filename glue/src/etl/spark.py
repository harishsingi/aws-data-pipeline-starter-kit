"""Spark / Glue session bootstrap for the ETL job."""

from __future__ import annotations


def create_spark_session():
    """Build the SparkSession via GlueContext with sane pipeline defaults."""
    from awsglue.context import GlueContext
    from pyspark.context import SparkContext

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    # Overwriting a single dt= partition must not wipe sibling partitions.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    # Parquet timestamps stay readable by Athena/Redshift Spectrum.
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS")
    return glue_context, spark
