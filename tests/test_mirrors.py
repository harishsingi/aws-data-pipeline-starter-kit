"""Mirror-sync tests: deployable copies must agree with the shared package.

The Lambda (``lambda/src``) and Glue (``glue/src/etl``) deployment zips are
self-contained, so they carry mirrored copies of the validation rules, the
retry behaviour and the constants from ``src/pipeline_kit``. These tests
fail loudly if a mirror drifts from the canonical implementation.
"""

import os
import random
import sys
import types
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))
sys.path.insert(0, os.path.join(REPO_ROOT, "glue", "src"))
sys.path.insert(0, os.path.join(REPO_ROOT, "lambda", "src"))

# --- stub boto3 (lambda app imports it; validation/retry/config do not) ---
_fake_boto3 = types.ModuleType("boto3")
_fake_boto3.client = lambda *a, **k: None
sys.modules.setdefault("boto3", _fake_boto3)

# --- Spark/AWS stubs, shared with test_glue_modules (installed once) ---
from tests.stubs import install as _install_stubs  # noqa: E402

_install_stubs()

import pipeline_kit.config as pk_config  # noqa: E402
import pipeline_kit.retry as pk_retry  # noqa: E402
import pipeline_kit.validation as pk_validation  # noqa: E402
import config as lambda_config  # noqa: E402
import retry as lambda_retry  # noqa: E402
import validation as lambda_validation  # noqa: E402
import etl.config as etl_config  # noqa: E402
import etl.retry as etl_retry  # noqa: E402


CORPUS = [
    {
        "order_id": "ORD-000001",
        "order_ts": "2026-09-25T10:15:30Z",
        "customer_id": "C0001",
        "product_id": "P001",
        "quantity": 3,
        "unit_price": 49.99,
        "discount": 5.0,
    },
    {
        "order_id": "ORD-000002",
        "order_ts": "2026-09-25T10:15:30+00:00",
        "customer_id": "C0002",
        "product_id": "P002",
        "quantity": 1000,
        "unit_price": 0,
        "discount": 0,
    },
    {"order_id": "", "order_ts": "not-a-date", "customer_id": "C1",
     "product_id": "P1", "quantity": 0, "unit_price": -5, "discount": 99},
    {"order_id": "ORD-3", "customer_id": "C1", "product_id": "P1",
     "quantity": 2.5, "unit_price": "49.99", "discount": True},
    {"order_id": "ORD-4", "order_ts": "2026-09-25T10:00:00Z",
     "customer_id": "C1", "product_id": "P1", "quantity": 3.0,
     "unit_price": 10, "discount": 10},
    {"order_id": "ORD-5", "order_ts": None, "customer_id": None,
     "product_id": "P1", "quantity": None, "unit_price": None, "discount": None},
    ["not", "a", "dict"],
    {},
]


class TestValidationMirrors(unittest.TestCase):
    def test_lambda_validation_matches_canonical(self):
        for rec in CORPUS:
            with self.subTest(rec=rec):
                self.assertEqual(
                    lambda_validation.validate_record(rec),
                    pk_validation.validate_record(rec),
                )

    def test_timestamp_parser_matches(self):
        for value in ("2026-09-25T10:15:30Z", "2026-09-25T10:15:30+00:00",
                      "bad", "", None, 123):
            with self.subTest(value=value):
                a = lambda_validation.parse_order_timestamp(value)
                b = pk_validation.parse_order_timestamp(value)
                self.assertEqual(
                    None if a is None else a.isoformat(),
                    None if b is None else b.isoformat(),
                )


class TestRetryMirrors(unittest.TestCase):
    def _scenario(self, retry_mod):
        sleeps: list[float] = []
        attempts = {"n": 0}

        @retry_mod.retry_with_backoff(
            retry_mod.RetryConfig(max_attempts=3, base_delay_seconds=0.1,
                                  max_delay_seconds=1.0),
            sleep=sleeps.append,
            rng=random.Random(5),
        )
        def flaky():
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise ConnectionError("flaky")
            return "ok"

        return flaky(), attempts["n"], len(sleeps)

    def test_lambda_retry_matches_canonical(self):
        self.assertEqual(self._scenario(lambda_retry), self._scenario(pk_retry))

    def test_etl_retry_matches_canonical(self):
        self.assertEqual(self._scenario(etl_retry), self._scenario(pk_retry))

    def test_retry_exhaustion_matches(self):
        for mod in (lambda_retry, etl_retry, pk_retry):
            with self.subTest(mod=mod.__name__):
                @mod.retry_with_backoff(
                    mod.RetryConfig(max_attempts=2),
                    sleep=lambda s: None,
                    rng=random.Random(1),
                )
                def always_fails():
                    raise ConnectionError("down")

                with self.assertRaises(mod.RetryExhaustedError) as ctx:
                    always_fails()
                self.assertEqual(ctx.exception.attempts, 2)


class TestConstantMirrors(unittest.TestCase):
    def test_record_contract_constants(self):
        self.assertEqual(lambda_config.REQUIRED_FIELDS, pk_config.REQUIRED_FIELDS)
        self.assertEqual(
            (lambda_config.QUANTITY_MIN, lambda_config.QUANTITY_MAX),
            (pk_config.QUANTITY_MIN, pk_config.QUANTITY_MAX),
        )
        self.assertEqual(lambda_config.LANDING_PREFIX, pk_config.LANDING_PREFIX)
        self.assertEqual(lambda_config.VALIDATED_PREFIX, pk_config.VALIDATED_PREFIX)
        self.assertEqual(lambda_config.QUARANTINE_PREFIX, pk_config.QUARANTINE_PREFIX)
        self.assertEqual(
            lambda_config.METRICS_NAMESPACE_DEFAULT, pk_config.METRICS_NAMESPACE_DEFAULT
        )

    def test_glue_constants(self):
        self.assertEqual(etl_config.EFFECTIVE_TO_OPEN, pk_config.EFFECTIVE_TO_OPEN)
        self.assertEqual(
            etl_config.ORPHAN_FAILURE_THRESHOLD, pk_config.ORPHAN_FAILURE_THRESHOLD
        )

    def test_retry_config_validation_matches(self):
        for mod in (lambda_retry, etl_retry, pk_retry):
            with self.subTest(mod=mod.__name__):
                with self.assertRaises(ValueError):
                    mod.RetryConfig(max_attempts=0)


if __name__ == "__main__":
    unittest.main()
