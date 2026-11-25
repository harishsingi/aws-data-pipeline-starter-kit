"""Tests for pipeline_kit.config / errors (stdlib unittest only).

Run from the repo root:  python3 -m unittest discover -s tests -v
(or: python3 tests/run_tests.py)
"""

import os
import sys
import unittest
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from pipeline_kit.config import (  # noqa: E402
    EFFECTIVE_TO_OPEN,
    ORPHAN_FAILURE_THRESHOLD,
    QUANTITY_MAX,
    QUANTITY_MIN,
    PipelineConfig,
    parse_run_date,
    validate_bucket_name,
)
from pipeline_kit.errors import ConfigurationError  # noqa: E402


class TestConstants(unittest.TestCase):
    def test_bounds_are_sane(self):
        self.assertEqual((QUANTITY_MIN, QUANTITY_MAX), (1, 1000))
        self.assertEqual(ORPHAN_FAILURE_THRESHOLD, 0.05)
        self.assertEqual(EFFECTIVE_TO_OPEN, "9999-12-31")


class TestParseRunDate(unittest.TestCase):
    def test_valid(self):
        self.assertEqual(parse_run_date("2026-09-25"), date(2026, 9, 25))

    def test_invalid(self):
        for bad in ("25-09-2026", "2026-13-01", "2026-02-30", "", "yesterday", None):
            with self.subTest(value=bad):
                with self.assertRaises(ConfigurationError):
                    parse_run_date(bad)


class TestValidateBucketName(unittest.TestCase):
    def test_valid(self):
        self.assertEqual(validate_bucket_name("my-raw-bucket", field="RAW_BUCKET"), "my-raw-bucket")

    def test_blank(self):
        with self.assertRaises(ConfigurationError):
            validate_bucket_name("   ", field="RAW_BUCKET")

    def test_uppercase_and_bad_chars(self):
        for bad in ("MyBucket", "my_bucket", "my..bucket", "-mybucket", "a"):
            with self.subTest(value=bad):
                with self.assertRaises(ConfigurationError):
                    validate_bucket_name(bad, field="RAW_BUCKET")


class TestPipelineConfig(unittest.TestCase):
    def _mapping(self):
        return {
            "RAW_BUCKET": "kit-dev-raw",
            "CURATED_BUCKET": "kit-dev-curated",
            "MARTS_BUCKET": "kit-dev-marts",
            "DATABASE": "kit_dev",
            "RUN_DATE": "2026-09-25",
        }

    def test_from_mapping(self):
        cfg = PipelineConfig.from_mapping(self._mapping())
        self.assertEqual(cfg.run_date, date(2026, 9, 25))
        self.assertEqual(cfg.run_date_str, "2026-09-25")

    def test_from_mapping_missing_key(self):
        mapping = self._mapping()
        del mapping["DATABASE"]
        with self.assertRaises(ConfigurationError):
            PipelineConfig.from_mapping(mapping)

    def test_from_mapping_bad_run_date(self):
        mapping = self._mapping()
        mapping["RUN_DATE"] = "Sept 25"
        with self.assertRaises(ConfigurationError):
            PipelineConfig.from_mapping(mapping)

    def test_from_env_defaults_run_date_to_today(self):
        env = {
            "RAW_BUCKET": "kit-dev-raw",
            "CURATED_BUCKET": "kit-dev-curated",
            "MARTS_BUCKET": "kit-dev-marts",
            "DATABASE": "kit_dev",
        }
        cfg = PipelineConfig.from_env(env)
        self.assertEqual(cfg.run_date, date.today())

    def test_frozen(self):
        cfg = PipelineConfig.from_mapping(self._mapping())
        with self.assertRaises(AttributeError):
            cfg.raw_bucket = "other"


if __name__ == "__main__":
    unittest.main()
