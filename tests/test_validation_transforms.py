"""Tests for pipeline_kit.validation / transforms / metrics (stdlib only)."""

import os
import sys
import unittest
from datetime import date
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from pipeline_kit.transforms import (  # noqa: E402
    customer_surrogate_key,
    date_sk,
    is_open_ended,
    line_total,
    orphan_rate_exceeds_threshold,
    product_surrogate_key,
    quarter_from_month,
)
from pipeline_kit.validation import (  # noqa: E402
    check_required_columns,
    derive_dt_from_path,
    parse_order_timestamp,
    validate_record,
)
from pipeline_kit.metrics import RowCounts  # noqa: E402


def valid_record() -> dict:
    return {
        "order_id": "ORD-000001",
        "order_ts": "2026-09-25T10:15:30Z",
        "customer_id": "C0001",
        "product_id": "P001",
        "quantity": 3,
        "unit_price": 49.99,
        "discount": 5.0,
    }


class TestValidateRecord(unittest.TestCase):
    def test_valid(self):
        self.assertEqual(validate_record(valid_record()), [])

    def test_missing_and_none(self):
        rec = valid_record()
        del rec["customer_id"]
        rec["product_id"] = None
        errors = validate_record(rec)
        self.assertIn("missing_field:customer_id", errors)
        self.assertIn("missing_field:product_id", errors)

    def test_bad_quantity(self):
        for bad in (0, 1001, -2, 2.5, "3", True):
            with self.subTest(quantity=bad):
                rec = valid_record()
                rec["quantity"] = bad
                self.assertIn("invalid_quantity", validate_record(rec))

    def test_quantity_boundaries(self):
        for good in (1, 1000, 3.0):
            with self.subTest(quantity=good):
                rec = valid_record()
                rec["quantity"] = good
                self.assertEqual(validate_record(rec), [])

    def test_discount_rules(self):
        rec = valid_record()
        rec["discount"] = 60.0
        self.assertIn("invalid_discount", validate_record(rec))
        rec["discount"] = rec["unit_price"]
        self.assertEqual(validate_record(rec), [])
        rec["discount"] = -0.5
        self.assertIn("invalid_discount", validate_record(rec))

    def test_bad_timestamp(self):
        for bad in ("not-a-date", "2026-13-99T99:99:99Z", "", 123456):
            with self.subTest(order_ts=bad):
                rec = valid_record()
                rec["order_ts"] = bad
                self.assertIn("invalid_order_ts", validate_record(rec))

    def test_empty_identifier(self):
        for bad in ("", "   ", 123):
            with self.subTest(order_id=bad):
                rec = valid_record()
                rec["order_id"] = bad
                self.assertIn("invalid_order_id", validate_record(rec))

    def test_non_dict(self):
        self.assertEqual(validate_record(["not", "a", "dict"]), ["not_an_object"])

    def test_multiple_errors_all_reported(self):
        rec = valid_record()
        rec["quantity"] = 0
        rec["discount"] = 999.0
        del rec["customer_id"]
        errors = validate_record(rec)
        self.assertIn("invalid_quantity", errors)
        self.assertIn("invalid_discount", errors)
        self.assertIn("missing_field:customer_id", errors)

    def test_parse_order_timestamp(self):
        self.assertIsNotNone(parse_order_timestamp("2026-09-25T10:15:30Z"))
        self.assertIsNotNone(parse_order_timestamp("2026-09-25T10:15:30+00:00"))
        self.assertIsNone(parse_order_timestamp("not-a-date"))
        self.assertIsNone(parse_order_timestamp(None))


class TestDeriveDtFromPath(unittest.TestCase):
    def test_found(self):
        self.assertEqual(
            derive_dt_from_path("landing/orders/dt=2026-09-25/orders.jsonl"),
            "2026-09-25",
        )

    def test_missing(self):
        self.assertIsNone(derive_dt_from_path("orders.jsonl"))
        self.assertIsNone(derive_dt_from_path("landing/orders/orders.jsonl"))

    def test_malformed_segment_ignored(self):
        self.assertIsNone(derive_dt_from_path("landing/orders/dt=25-09-2026/x.jsonl"))


class TestCheckRequiredColumns(unittest.TestCase):
    def test_ok_and_missing(self):
        self.assertEqual(check_required_columns(["a", "b"], ["a", "b"]), [])
        self.assertEqual(check_required_columns(["a"], ["a", "b"]), ["b"])


class TestTransforms(unittest.TestCase):
    def test_customer_surrogate_key_is_sha256_of_id_and_date(self):
        import hashlib

        expected = hashlib.sha256("C0001||2026-01-01".encode()).hexdigest()
        self.assertEqual(customer_surrogate_key("C0001", "2026-01-01"), expected)
        self.assertEqual(customer_surrogate_key("C0001", date(2026, 1, 1)), expected)
        # deterministic and version-sensitive
        self.assertNotEqual(
            customer_surrogate_key("C0001", "2026-01-01"),
            customer_surrogate_key("C0001", "2026-01-02"),
        )
        with self.assertRaises(ValueError):
            customer_surrogate_key("   ", "2026-01-01")

    def test_product_surrogate_key(self):
        import hashlib

        self.assertEqual(
            product_surrogate_key("P001"),
            hashlib.sha256(b"P001").hexdigest(),
        )
        with self.assertRaises(ValueError):
            product_surrogate_key("")

    def test_date_sk_and_quarter(self):
        self.assertEqual(date_sk(date(2026, 9, 25)), 20260925)
        for month, quarter in ((1, 1), (3, 1), (4, 2), (6, 2), (7, 3), (12, 4)):
            with self.subTest(month=month):
                self.assertEqual(quarter_from_month(month), quarter)
        with self.assertRaises(ValueError):
            quarter_from_month(0)
        with self.assertRaises(ValueError):
            quarter_from_month(13)

    def test_line_total_exact_decimal(self):
        self.assertEqual(
            line_total(3, Decimal("49.99"), Decimal("5.00")), Decimal("134.97")
        )
        self.assertEqual(
            line_total(2, Decimal("19.99"), Decimal("0")), Decimal("39.98")
        )
        with self.assertRaises(ValueError):
            line_total(-1, Decimal("10"), Decimal("0"))

    def test_orphan_threshold(self):
        self.assertFalse(orphan_rate_exceeds_threshold(5, 100))  # exactly 5% is OK
        self.assertTrue(orphan_rate_exceeds_threshold(6, 100))
        self.assertFalse(orphan_rate_exceeds_threshold(0, 0))
        self.assertFalse(orphan_rate_exceeds_threshold(10, 100, threshold=0.10))
        with self.assertRaises(ValueError):
            orphan_rate_exceeds_threshold(-1, 100)

    def test_is_open_ended(self):
        self.assertTrue(is_open_ended("9999-12-31"))
        self.assertFalse(is_open_ended("2026-09-24"))


class TestRowCounts(unittest.TestCase):
    def test_rates_and_lost(self):
        counts = RowCounts(rows_in=100, rows_out=90, quarantined=5, duplicates_removed=5)
        self.assertAlmostEqual(counts.rejected_rate, 0.05)
        self.assertEqual(counts.rows_lost, 0)
        self.assertEqual(RowCounts(rows_in=0).rejected_rate, 0.0)

    def test_negative_rejected(self):
        with self.assertRaises(ValueError):
            RowCounts(rows_in=-1)

    def test_log_summary_does_not_raise(self):
        import logging

        RowCounts(rows_in=10, rows_out=9, quarantined=1).log_summary(
            logging.getLogger("test"), "stage"
        )


if __name__ == "__main__":
    unittest.main()
