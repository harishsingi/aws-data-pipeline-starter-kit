"""Shared Spark/AWS stubs for the Glue tests (stdlib unittest only).

``pyspark``, ``awsglue`` and ``boto3`` do not exist in this environment, so
they are stubbed in ``sys.modules`` before the ``etl`` modules are
imported -- the same pattern ``lambda/tests/test_validation.py`` uses for
``boto3``.

Both ``test_glue_modules.py`` and ``test_mirrors.py`` import this module,
so the stubs are installed exactly once (``install()`` is idempotent) and
there is a single ``FAKE_JOB_ARGS`` dict for the ``parse_args`` tests to
mutate.
"""

import os
import sys
import types

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

FAKE_JOB_ARGS = {
    "JOB_NAME": "test-etl-job",
    "RAW_BUCKET": "kit-dev-raw",
    "CURATED_BUCKET": "kit-dev-curated",
    "MARTS_BUCKET": "kit-dev-marts",
    "DATABASE": "kit_dev",
    "RUN_DATE": "2026-09-25",
}

_INSTALLED = False


def _stub_module(name):
    module = types.ModuleType(name)
    sys.modules[name] = module
    return module


class _StubType:
    """Accepts any constructor args; reprs readably."""

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __repr__(self):
        return f"<stub {self.__class__.__name__}{self.args!r}>"


def _stub_fn(*args, **kwargs):
    return _StubType(*args, **kwargs)


def install():
    """Install the pyspark/awsglue stubs (no-op when already installed)."""
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True

    sys.path.insert(0, os.path.join(REPO_ROOT, "glue", "src"))
    sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

    pyspark = _stub_module("pyspark")
    pyspark_context = _stub_module("pyspark.context")
    pyspark_context.SparkContext = _StubType
    pyspark_sql = _stub_module("pyspark.sql")
    pyspark_sql_types = _stub_module("pyspark.sql.types")
    for _name in ("StructType", "StructField", "StringType", "IntegerType",
                  "DecimalType", "TimestampType", "DateType", "BooleanType"):
        setattr(pyspark_sql, _name, _StubType)
        setattr(pyspark_sql_types, _name, _StubType)
    pyspark_sql_functions = _stub_module("pyspark.sql.functions")
    for _name in ("col", "lit", "row_number", "to_timestamp", "broadcast",
                  "concat_ws", "date_format", "date_sub", "explode", "sequence",
                  "sha2", "to_date"):
        setattr(pyspark_sql_functions, _name, _stub_fn)
    pyspark_sql_window = _stub_module("pyspark.sql.window")
    pyspark_sql_window.Window = _StubType

    awsglue = _stub_module("awsglue")
    awsglue_context = _stub_module("awsglue.context")
    awsglue_context.GlueContext = _StubType
    awsglue_job = _stub_module("awsglue.job")
    awsglue_job.Job = _StubType
    awsglue_utils = _stub_module("awsglue.utils")
    awsglue_utils.getResolvedOptions = lambda argv, names: dict(FAKE_JOB_ARGS)
