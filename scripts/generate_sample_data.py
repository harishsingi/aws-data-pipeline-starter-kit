#!/usr/bin/env python3
"""Generate deterministic sample data for the Data Pipeline Starter Kit.

Thin CLI wrapper: the logic lives in ``src/pipeline_kit/generator.py``
so it can be unit-tested without a subprocess.

Writes reference data and per-day order files under ``<out>/raw/``:

  <out>/raw/reference/customers/dt=<today>/customers.json
  <out>/raw/reference/products/dt=<today>/products.json
  <out>/raw/landing/orders/dt=<date>/orders.jsonl      (one file per day)

All data is synthetic (invented names, ``example.com`` emails, generic
product names) and all randomness comes from ``random.Random(seed)``, so
the output is fully reproducible for a given seed. Stdlib only.

Example:
    python3 scripts/generate_sample_data.py --days 2 --orders-per-day 50
"""

import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from pipeline_kit.errors import ConfigurationError  # noqa: E402
from pipeline_kit.generator import (  # noqa: E402
    DEFAULT_CUSTOMERS,
    DEFAULT_DAYS,
    DEFAULT_ORDERS_PER_DAY,
    DEFAULT_PRODUCTS,
    DEFAULT_SEED,
    GeneratorOptions,
    generate_dataset,
)

logger = logging.getLogger("generate_sample_data")


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate deterministic sample customers, products and orders."
    )
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS,
                        help=f"number of trailing days of orders (default: {DEFAULT_DAYS})")
    parser.add_argument("--orders-per-day", type=int, default=DEFAULT_ORDERS_PER_DAY,
                        help=f"orders to generate per day (default: {DEFAULT_ORDERS_PER_DAY})")
    parser.add_argument("--customers", type=int, default=DEFAULT_CUSTOMERS,
                        help=f"number of customers (default: {DEFAULT_CUSTOMERS})")
    parser.add_argument("--products", type=int, default=DEFAULT_PRODUCTS,
                        help=f"number of products (default: {DEFAULT_PRODUCTS})")
    parser.add_argument("--out", default="sample_data",
                        help="output directory (default: sample_data)")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED,
                        help=f"random seed for reproducibility (default: {DEFAULT_SEED})")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    args = parse_args(argv)
    try:
        options = GeneratorOptions(
            days=args.days,
            orders_per_day=args.orders_per_day,
            customers=args.customers,
            products=args.products,
            seed=args.seed,
            out=args.out,
        )
    except ConfigurationError as exc:
        logger.error("%s", exc)
        return 2
    summary = generate_dataset(options)
    logger.info(
        "Done: %d orders across %d day(s) in %s",
        summary.orders,
        summary.days,
        os.path.abspath(summary.out_dir),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
