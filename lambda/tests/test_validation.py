"""Unit tests for ``lambda/src/app.py`` -- stdlib ``unittest`` only.

``boto3`` is stubbed in ``sys.modules`` BEFORE ``app`` is imported, so the
whole suite runs with no AWS credentials, no network, and no third-party
packages. Run from the repo root::

    python3 -m unittest discover -s lambda/tests
"""

import io
import json
import os
import sys
import types
import unittest

# ---------------------------------------------------------------------------
# Make ``lambda/src/app.py`` importable (``lambda`` is a Python keyword, so
# the package cannot be imported by name -- load the module off sys.path).
# ---------------------------------------------------------------------------
REPO_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.insert(0, os.path.join(REPO_ROOT, "lambda", "src"))


# ---------------------------------------------------------------------------
# boto3 stub -- must be installed before ``import app``
# ---------------------------------------------------------------------------
class _FakeS3Client:
    def __init__(self):
        self.store = {}  # (bucket, key) -> bytes
        self.put_calls = []  # list of (bucket, key)

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.store[(Bucket, Key)])}

    def put_object(self, Bucket, Key, Body):
        if isinstance(Body, str):
            Body = Body.encode("utf-8")
        elif hasattr(Body, "read"):
            Body = Body.read()
        self.store[(Bucket, Key)] = Body
        self.put_calls.append((Bucket, Key))
        return {}


class _FakeCloudWatchClient:
    def __init__(self):
        self.metric_calls = []  # list of {"Namespace": ..., "MetricData": ...}

    def put_metric_data(self, Namespace, MetricData):
        self.metric_calls.append({"Namespace": Namespace, "MetricData": MetricData})
        return {}


class _FakeBoto3Module(types.ModuleType):
    def __init__(self):
        super().__init__("boto3")
        self.s3 = _FakeS3Client()
        self.cloudwatch = _FakeCloudWatchClient()

    def client(self, service_name, **kwargs):
        if service_name == "s3":
            return self.s3
        if service_name == "cloudwatch":
            return self.cloudwatch
        raise ValueError(f"unexpected service: {service_name}")


_fake_boto3 = _FakeBoto3Module()
sys.modules["boto3"] = _fake_boto3

import app  # noqa: E402  (imported after the boto3 stub on purpose)


# ---------------------------------------------------------------------------
# validate_record tests
# ---------------------------------------------------------------------------
class TestValidateRecord(unittest.TestCase):
    def _valid(self):
        return {
            "order_id": "ORD-000001",
            "order_ts": "2026-09-25T10:15:30Z",
            "customer_id": "C0001",
            "product_id": "P001",
            "quantity": 3,
            "unit_price": 49.99,
            "discount": 5.0,
        }

    def test_valid_record(self):
        self.assertEqual(app.validate_record(self._valid()), [])

    def test_valid_record_with_offset_timestamp(self):
        rec = self._valid()
        rec["order_ts"] = "2026-09-25T10:15:30+00:00"
        self.assertEqual(app.validate_record(rec), [])

    def test_valid_record_with_integral_float_quantity(self):
        rec = self._valid()
        rec["quantity"] = 3.0
        self.assertEqual(app.validate_record(rec), [])

    def test_missing_field(self):
        rec = self._valid()
        del rec["customer_id"]
        errors = app.validate_record(rec)
        self.assertIn("missing_field:customer_id", errors)

    def test_none_field_counts_as_missing(self):
        rec = self._valid()
        rec["product_id"] = None
        errors = app.validate_record(rec)
        self.assertIn("missing_field:product_id", errors)

    def test_bad_quantity(self):
        for bad in (0, 1001, -2, 2.5, "3", True):
            with self.subTest(quantity=bad):
                rec = self._valid()
                rec["quantity"] = bad
                self.assertIn("invalid_quantity", app.validate_record(rec))

    def test_quantity_boundaries(self):
        for good in (1, 1000):
            with self.subTest(quantity=good):
                rec = self._valid()
                rec["quantity"] = good
                self.assertEqual(app.validate_record(rec), [])

    def test_discount_greater_than_unit_price(self):
        rec = self._valid()
        rec["discount"] = 60.0
        self.assertIn("invalid_discount", app.validate_record(rec))

    def test_discount_equal_to_unit_price_is_valid(self):
        rec = self._valid()
        rec["discount"] = rec["unit_price"]
        self.assertEqual(app.validate_record(rec), [])

    def test_negative_discount(self):
        rec = self._valid()
        rec["discount"] = -0.5
        self.assertIn("invalid_discount", app.validate_record(rec))

    def test_negative_unit_price(self):
        rec = self._valid()
        rec["unit_price"] = -1.0
        self.assertIn("invalid_unit_price", app.validate_record(rec))

    def test_bad_timestamp(self):
        for bad in ("not-a-date", "2026-13-99T99:99:99Z", "", 123456):
            with self.subTest(order_ts=bad):
                rec = self._valid()
                rec["order_ts"] = bad
                self.assertIn("invalid_order_ts", app.validate_record(rec))

    def test_empty_order_id(self):
        for bad in ("", "   ", 123):
            with self.subTest(order_id=bad):
                rec = self._valid()
                rec["order_id"] = bad
                self.assertIn("invalid_order_id", app.validate_record(rec))

    def test_non_dict_input(self):
        self.assertEqual(app.validate_record(["not", "a", "dict"]), ["not_an_object"])

    def test_multiple_errors_all_reported(self):
        rec = self._valid()
        rec["quantity"] = 0
        rec["discount"] = 999.0
        del rec["customer_id"]
        errors = app.validate_record(rec)
        self.assertIn("invalid_quantity", errors)
        self.assertIn("invalid_discount", errors)
        self.assertIn("missing_field:customer_id", errors)


# ---------------------------------------------------------------------------
# handler tests
# ---------------------------------------------------------------------------
class TestHandler(unittest.TestCase):
    def setUp(self):
        _fake_boto3.s3.store.clear()
        _fake_boto3.s3.put_calls.clear()
        _fake_boto3.cloudwatch.metric_calls.clear()
        self._old_env = dict(os.environ)
        os.environ["RAW_BUCKET"] = "test-raw-bucket"
        os.environ["METRICS_NAMESPACE"] = "Test/Ingestion"

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._old_env)

    def _event(self, bucket, key):
        return {
            "Records": [{"s3": {"bucket": {"name": bucket}, "object": {"key": key}}}]
        }

    def _written_keys(self):
        return [key for (_bucket, key) in _fake_boto3.s3.put_calls]

    def _read_jsonl(self, bucket, key):
        body = _fake_boto3.s3.store[(bucket, key)].decode("utf-8")
        return [json.loads(line) for line in body.splitlines() if line.strip()]

    def test_handler_routes_valid_and_invalid_records(self):
        bucket = "test-raw-bucket"
        key = "landing/orders/dt=2026-09-25/orders-0001.jsonl"
        valid = {
            "order_id": "ORD-000001",
            "order_ts": "2026-09-25T10:15:30Z",
            "customer_id": "C0001",
            "product_id": "P001",
            "quantity": 2,
            "unit_price": 19.99,
            "discount": 1.5,
        }
        invalid = {
            "order_id": "",  # empty identifier
            "order_ts": "not-a-date",
            "customer_id": "C0002",
            "product_id": "P002",
            "quantity": 0,  # out of range
            "unit_price": -5,  # negative
            "discount": 99,  # exceeds unit_price
        }
        # one blank line in the middle must be skipped, not counted
        body = json.dumps(valid) + "\n\n" + json.dumps(invalid) + "\n"
        _fake_boto3.s3.store[(bucket, key)] = body.encode("utf-8")

        summary = app.handler(self._event(bucket, key), None)

        self.assertEqual(summary["files_processed"], 1)
        self.assertEqual(summary["files_failed"], 0)
        self.assertEqual(summary["valid_records"], 1)
        self.assertEqual(summary["invalid_records"], 1)

        validated = [k for k in self._written_keys() if k.startswith("validated/orders/dt=")]
        quarantined = [
            k for k in self._written_keys() if k.startswith("quarantine/orders/dt=")
        ]
        self.assertEqual(len(validated), 1, "expected one validated output object")
        self.assertEqual(len(quarantined), 1, "expected one quarantine output object")

        good_records = self._read_jsonl(bucket, validated[0])
        self.assertEqual(len(good_records), 1)
        self.assertEqual(good_records[0]["order_id"], "ORD-000001")
        self.assertNotIn("_validation_errors", good_records[0])

        bad_records = self._read_jsonl(bucket, quarantined[0])
        self.assertEqual(len(bad_records), 1)
        self.assertIn("_validation_errors", bad_records[0])
        self.assertTrue(bad_records[0]["_validation_errors"])
        self.assertEqual(bad_records[0]["order_id"], "")

        # metrics were emitted with the configured namespace
        self.assertTrue(_fake_boto3.cloudwatch.metric_calls)
        last = _fake_boto3.cloudwatch.metric_calls[-1]
        self.assertEqual(last["Namespace"], "Test/Ingestion")
        values = {m["MetricName"]: m["Value"] for m in last["MetricData"]}
        self.assertEqual(values["ValidRecords"], 1)
        self.assertEqual(values["InvalidRecords"], 1)
        self.assertEqual(values["ProcessedFiles"], 1)

    def test_handler_marks_malformed_json_lines_invalid(self):
        bucket = "test-raw-bucket"
        key = "landing/orders/dt=2026-09-25/orders-0002.jsonl"
        _fake_boto3.s3.store[(bucket, key)] = b"{this is not json}\n"

        summary = app.handler(self._event(bucket, key), None)

        self.assertEqual(summary["valid_records"], 0)
        self.assertEqual(summary["invalid_records"], 1)
        quarantined = [
            k for k in self._written_keys() if k.startswith("quarantine/orders/dt=")
        ]
        self.assertEqual(len(quarantined), 1)
        bad_records = self._read_jsonl(bucket, quarantined[0])
        self.assertEqual(bad_records[0]["_validation_errors"], ["invalid_json"])

    def test_handler_skips_keys_outside_landing_prefix(self):
        bucket = "test-raw-bucket"
        key = "reference/customers/dt=2026-09-25/customers.json"
        _fake_boto3.s3.store[(bucket, key)] = b"{}\n"

        summary = app.handler(self._event(bucket, key), None)

        self.assertEqual(summary["files_processed"], 0)
        self.assertEqual(self._written_keys(), [])


if __name__ == "__main__":
    unittest.main()
