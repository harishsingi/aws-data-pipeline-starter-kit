"""Tests for pipeline_kit.generator (stdlib unittest only)."""

import json
import os
import random
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from pipeline_kit.errors import ConfigurationError  # noqa: E402
from pipeline_kit.generator import (  # noqa: E402
    GeneratorOptions,
    generate_customers,
    generate_dataset,
    generate_orders,
    generate_products,
)
from pipeline_kit.validation import validate_record  # noqa: E402


class TestGeneratorOptions(unittest.TestCase):
    def test_invalid_counts(self):
        for kwargs in ({"days": 0}, {"orders_per_day": 0}, {"customers": 0}, {"products": -1}):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ConfigurationError):
                    GeneratorOptions(**kwargs)

    def test_blank_out(self):
        with self.assertRaises(ConfigurationError):
            GeneratorOptions(out="  ")


class TestGenerators(unittest.TestCase):
    def test_customers_unique_emails_and_ids(self):
        customers = generate_customers(random.Random(42), 50)
        self.assertEqual(len(customers), 50)
        self.assertEqual(len({c["email"] for c in customers}), 50)
        self.assertEqual(len({c["customer_id"] for c in customers}), 50)
        self.assertTrue(all(c["email"].endswith("@example.com") for c in customers))

    def test_products_cycle_categories(self):
        products = generate_products(random.Random(42), 8)
        self.assertEqual(
            [p["category"] for p in products],
            ["electronics", "apparel", "home", "sports"] * 2,
        )
        self.assertTrue(all(p["unit_price"] > 0 for p in products))

    def test_orders_pass_validation(self):
        rng = random.Random(42)
        customers = generate_customers(rng, 10)
        products = generate_products(rng, 10)
        orders = generate_orders(rng, customers, products, date(2026, 9, 25), 50, start_seq=1)
        self.assertEqual(len(orders), 50)
        self.assertEqual(len({o["order_id"] for o in orders}), 50)
        for order in orders:
            self.assertEqual(validate_record(order), [], msg=str(order))
            self.assertLessEqual(order["discount"], order["unit_price"])
            self.assertTrue(order["order_ts"].startswith("2026-09-25T"))

    def test_orders_reject_empty_references(self):
        rng = random.Random(1)
        with self.assertRaises(ConfigurationError):
            generate_orders(rng, [], generate_products(rng, 5), date(2026, 9, 25), 5, 1)

    def test_deterministic_same_seed(self):
        rng1, rng2 = random.Random(99), random.Random(99)
        c1 = generate_customers(rng1, 20)
        c2 = generate_customers(rng2, 20)
        self.assertEqual(c1, c2)

    def test_different_seed_differs(self):
        c1 = generate_customers(random.Random(1), 20)
        c2 = generate_customers(random.Random(2), 20)
        self.assertNotEqual(c1, c2)


class TestGenerateDataset(unittest.TestCase):
    def test_writes_expected_layout(self):
        with tempfile.TemporaryDirectory() as tmp:
            summary = generate_dataset(
                GeneratorOptions(days=2, orders_per_day=10, customers=5, products=4,
                                 seed=42, out=os.path.join(tmp, "sample_data")),
                today=date(2026, 9, 25),
            )
            self.assertEqual(summary.orders, 20)
            self.assertEqual(summary.days, 2)
            base = Path(tmp) / "sample_data" / "raw"
            self.assertTrue((base / "reference" / "customers" / "dt=2026-09-25" / "customers.json").exists())
            self.assertTrue((base / "reference" / "products" / "dt=2026-09-25" / "products.json").exists())
            day1 = base / "landing" / "orders" / "dt=2026-09-24" / "orders.jsonl"
            day2 = base / "landing" / "orders" / "dt=2026-09-25" / "orders.jsonl"
            self.assertTrue(day1.exists() and day2.exists())
            rows = [json.loads(line) for line in day2.read_text().splitlines()]
            self.assertEqual(len(rows), 10)
            for row in rows:
                self.assertEqual(validate_record(row), [])


if __name__ == "__main__":
    unittest.main()
