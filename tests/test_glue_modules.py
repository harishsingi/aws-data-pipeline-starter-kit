"""Tests for the Glue ``etl`` package (stdlib unittest only).

``pyspark`` / ``awsglue`` are stubbed via ``tests.stubs`` (see that module).
These tests cover argument parsing, S3 helpers and the quality gate; the
Spark transform bodies themselves are exercised by ``py_compile`` plus the
local demo path, not by a Spark cluster.
"""

import importlib.util
import logging
import os
import sys
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

logging.disable(logging.CRITICAL)

from tests.stubs import FAKE_JOB_ARGS, install  # noqa: E402

install()

import etl.catalog as etl_catalog  # noqa: E402
import etl.config as etl_config  # noqa: E402
import etl.gold as etl_gold  # noqa: E402
import etl.quality as etl_quality  # noqa: E402
import etl.s3utils as etl_s3utils  # noqa: E402
import etl.silver as etl_silver  # noqa: E402


def _load_driver():
    spec = importlib.util.spec_from_file_location(
        "etl_job_driver", os.path.join(REPO_ROOT, "glue", "etl_job.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestParseArgs(unittest.TestCase):
    def test_valid(self):
        args, run_date = etl_config.parse_args(["etl_job.py"])
        self.assertEqual(args["JOB_NAME"], "test-etl-job")
        self.assertEqual(run_date.isoformat(), "2026-09-25")

    def test_bad_run_date(self):
        FAKE_JOB_ARGS["RUN_DATE"] = "Sept 25"
        try:
            with self.assertRaises(ValueError):
                etl_config.parse_args(["etl_job.py"])
        finally:
            FAKE_JOB_ARGS["RUN_DATE"] = "2026-09-25"

    def test_blank_bucket(self):
        FAKE_JOB_ARGS["RAW_BUCKET"] = "   "
        try:
            with self.assertRaises(ValueError):
                etl_config.parse_args(["etl_job.py"])
        finally:
            FAKE_JOB_ARGS["RAW_BUCKET"] = "kit-dev-raw"


class TestS3Utils(unittest.TestCase):
    def test_list_dt_prefixes(self):
        class FakePaginator:
            def paginate(self, Bucket, Prefix, Delimiter):
                return [
                    {"CommonPrefixes": [
                        {"Prefix": "reference/customers/dt=2026-09-24/"},
                        {"Prefix": "reference/customers/dt=2026-09-25/"},
                        {"Prefix": "reference/customers/not-a-date/"},
                        {"Prefix": "reference/customers/dt=bad-date/"},
                    ]}
                ]

        class FakeS3:
            def get_paginator(self, name):
                return FakePaginator()

        dts = etl_s3utils.list_dt_prefixes(FakeS3(), "b", "reference/customers/")
        self.assertEqual(dts, ["2026-09-24", "2026-09-25"])

    def test_object_exists_true(self):
        class FakeS3:
            def head_object(self, Bucket, Key):
                return {}

        self.assertTrue(etl_s3utils.object_exists(FakeS3(), "b", "k"))

    def test_object_exists_false_on_404(self):
        class NotFound(Exception):
            def __init__(self):
                super().__init__("not found")
                self.response = {"Error": {"Code": "404"}}

        class FakeS3:
            def head_object(self, Bucket, Key):
                raise NotFound()

        # 404 is retried (3 attempts) then unwrapped to False.
        self.assertFalse(etl_s3utils.object_exists(FakeS3(), "b", "k"))

    def test_object_exists_raises_on_other_errors(self):
        class Forbidden(Exception):
            def __init__(self):
                super().__init__("forbidden")
                self.response = {"Error": {"Code": "403"}}

        class FakeS3:
            def head_object(self, Bucket, Key):
                raise Forbidden()

        with self.assertRaises(Exception):
            etl_s3utils.object_exists(FakeS3(), "b", "k")


class TestQualityGate(unittest.TestCase):
    def test_orphan_rate(self):
        etl_quality.check_orphan_rate(5, 100, 0.05)  # exactly 5% passes
        with self.assertRaises(ValueError):
            etl_quality.check_orphan_rate(6, 100, 0.05)
        etl_quality.check_orphan_rate(10, 100, 0.10)


class TestModulesImportAndExpose(unittest.TestCase):
    def test_all_modules_import(self):
        for module in (etl_config, etl_s3utils, etl_silver, etl_gold, etl_catalog, etl_quality):
            self.assertIsNotNone(module)

    def test_driver_imports_and_exposes_run_stage(self):
        driver = _load_driver()
        self.assertTrue(callable(driver.run_stage))
        self.assertTrue(callable(driver.main))
        # run_stage logs timing and returns the function result
        timings = {}
        result = driver.run_stage(logging.getLogger("test"), timings, "noop", lambda: 42)
        self.assertEqual(result, 42)
        self.assertIn("noop", timings)

    def test_config_constants(self):
        self.assertEqual(etl_config.EFFECTIVE_TO_OPEN, "9999-12-31")
        self.assertEqual(etl_config.ORPHAN_FAILURE_THRESHOLD, 0.05)
        self.assertEqual(etl_config.WRITE_PARTITIONS, 8)
        # schemas carry the expected fields
        self.assertEqual(len(etl_config.ORDERS_SCHEMA.args[0]), 7)
        self.assertEqual(len(etl_config.FACT_ORDERS_DDL), 10)


if __name__ == "__main__":
    unittest.main()
