#!/usr/bin/env python3
"""Generate deterministic sample data for the Data Pipeline Starter Kit.

Writes reference data and per-day order files under ``<out>/raw/``:

  <out>/raw/reference/customers/dt=<today>/customers.json
  <out>/raw/reference/products/dt=<today>/products.json
  <out>/raw/landing/orders/dt=<date>/orders.jsonl      (one file per day)

All randomness comes from ``random.Random(seed)``, so the output is fully
reproducible for a given seed. Stdlib only.

Example:
    python3 scripts/generate_sample_data.py --days 2 --orders-per-day 50
"""

import argparse
import json
import os
import random
from datetime import date, datetime, timedelta, timezone

# ---------------------------------------------------------------------------
# Name / catalog pools (fixed pools keep the generator deterministic and
# dependency-free)
# ---------------------------------------------------------------------------

FIRST_NAMES = [
    "Ava", "Liam", "Maya", "Noah", "Zoe", "Ethan", "Priya", "Lucas",
    "Sofia", "Mateo", "Aisha", "Owen", "Nina", "Kai", "Elena", "Ravi",
    "Hana", "Diego", "Ivy", "Omar",
]

LAST_NAMES = [
    "Sharma", "Garcia", "Kim", "Patel", "Nguyen", "Smith", "Khan",
    "Muller", "Rossi", "Dubois", "Tanaka", "Silva", "Okafor", "Ivanov",
    "Chen", "Ali", "Brown", "Kowalski", "Haddad", "Yilmaz",
]

SEGMENTS = ("consumer", "corporate", "enterprise")

CATEGORIES = ("electronics", "apparel", "home", "sports")

PRODUCT_NAMES = {
    "electronics": [
        "Wireless Headphones", "USB-C Hub", "Mechanical Keyboard",
        "Portable SSD 1TB", "Smart Speaker", "Action Camera",
        "Noise-Cancelling Earbuds", "4K Monitor 27in", "Power Bank 20K",
        "Robot Vacuum",
    ],
    "apparel": [
        "Cotton T-Shirt", "Denim Jacket", "Running Shoes",
        "Wool Sweater", "Yoga Leggings", "Canvas Backpack",
        "Linen Shirt", "Trail Boots", "Puffer Vest", "Knit Beanie",
    ],
    "home": [
        "Ceramic Vase Set", "LED Desk Lamp", "Memory Foam Pillow",
        "Cast Iron Skillet", "Bamboo Cutting Board", "Throw Blanket",
        "Air Purifier", "Espresso Maker", "Wall Clock", "Storage Ottoman",
    ],
    "sports": [
        "Yoga Mat Pro", "Dumbbell Set 20kg", "Insulated Water Bottle",
        "Tennis Racket", "Resistance Bands", "Camping Tent 2P",
        "Basketball", "Cycling Helmet", "Jump Rope", "Foam Roller",
    ],
}

# Typical price band per category: (low, high)
PRICE_BANDS = {
    "electronics": (29.99, 499.99),
    "apparel": (14.99, 149.99),
    "home": (9.99, 249.99),
    "sports": (12.99, 199.99),
}


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------


def generate_customers(rng: random.Random, count: int) -> list[dict]:
    """Generate customer reference records."""
    customers = []
    used_emails = set()
    for i in range(1, count + 1):
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        base_email = f"{first}.{last}@example.com".lower()
        email = base_email
        suffix = 2
        while email in used_emails:  # keep emails unique
            email = f"{first}.{last}{suffix}@example.com".lower()
            suffix += 1
        used_emails.add(email)
        customers.append(
            {
                "customer_id": f"C{i:04d}",
                "full_name": f"{first} {last}",
                "email": email,
                "segment": rng.choice(SEGMENTS),
            }
        )
    return customers


def generate_products(rng: random.Random, count: int) -> list[dict]:
    """Generate product reference records, cycling through categories."""
    products = []
    for i in range(1, count + 1):
        category = CATEGORIES[(i - 1) % len(CATEGORIES)]
        name_pool = PRODUCT_NAMES[category]
        # pool has 10 names; append a variant suffix once we lap it
        name = name_pool[(i - 1) % len(name_pool)]
        lap = (i - 1) // len(name_pool)
        if lap:
            name = f"{name} (Gen {lap + 1})"
        low, high = PRICE_BANDS[category]
        unit_price = round(rng.uniform(low, high), 2)
        products.append(
            {
                "product_id": f"P{i:03d}",
                "product_name": name,
                "category": category,
                "unit_price": unit_price,
            }
        )
    return products


def generate_orders(
    rng: random.Random,
    customers: list[dict],
    products: list[dict],
    day: date,
    orders_per_day: int,
    start_seq: int,
) -> list[dict]:
    """Generate one day of order records with timestamps spread through it."""
    orders = []
    for n in range(orders_per_day):
        customer = rng.choice(customers)
        product = rng.choice(products)
        quantity = rng.randint(1, 5)
        unit_price = product["unit_price"]
        # most orders have no discount; the rest get a small one
        if rng.random() < 0.7:
            discount = 0.0
        else:
            discount = round(rng.uniform(0.02, 0.15) * unit_price, 2)
            discount = min(discount, unit_price)  # never exceed unit_price
        # random second of the day, emitted as UTC ISO8601 with 'Z'
        second_of_day = rng.randint(0, 86_399)
        order_ts = datetime(
            day.year, day.month, day.day, tzinfo=timezone.utc
        ) + timedelta(seconds=second_of_day)
        orders.append(
            {
                "order_id": f"ORD-{start_seq + n:06d}",
                "order_ts": order_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "customer_id": customer["customer_id"],
                "product_id": product["product_id"],
                "quantity": quantity,
                "unit_price": unit_price,
                "discount": discount,
            }
        )
    return orders


def write_json(path: str, records: list[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(records, fh, indent=2)
        fh.write("\n")


def write_jsonl(path: str, records: list[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record) + "\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate deterministic sample customers, products and orders."
    )
    parser.add_argument("--days", type=int, default=7,
                        help="number of trailing days of orders (default: 7)")
    parser.add_argument("--orders-per-day", type=int, default=500,
                        help="orders to generate per day (default: 500)")
    parser.add_argument("--customers", type=int, default=200,
                        help="number of customers (default: 200)")
    parser.add_argument("--products", type=int, default=50,
                        help="number of products (default: 50)")
    parser.add_argument("--out", default="sample_data",
                        help="output directory (default: sample_data)")
    parser.add_argument("--seed", type=int, default=42,
                        help="random seed for reproducibility (default: 42)")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.days < 1:
        raise SystemExit("--days must be >= 1")
    if args.orders_per_day < 1:
        raise SystemExit("--orders-per-day must be >= 1")

    rng = random.Random(args.seed)
    today = datetime.now(timezone.utc).date()

    customers = generate_customers(rng, args.customers)
    products = generate_products(rng, args.products)

    ref_base = os.path.join(args.out, "raw", "reference")
    write_json(
        os.path.join(ref_base, "customers", f"dt={today.isoformat()}", "customers.json"),
        customers,
    )
    write_json(
        os.path.join(ref_base, "products", f"dt={today.isoformat()}", "products.json"),
        products,
    )

    first_day = today - timedelta(days=args.days - 1)
    seq = 1
    total_orders = 0
    for offset in range(args.days):
        day = first_day + timedelta(days=offset)
        orders = generate_orders(
            rng, customers, products, day, args.orders_per_day, start_seq=seq
        )
        seq += len(orders)
        total_orders += len(orders)
        write_jsonl(
            os.path.join(
                args.out, "raw", "landing", "orders",
                f"dt={day.isoformat()}", "orders.jsonl",
            ),
            orders,
        )

    out_abs = os.path.abspath(args.out)
    print("Sample data generated:")
    print(f"  output dir : {out_abs}")
    print(f"  date range : {first_day.isoformat()} .. {today.isoformat()} "
          f"({args.days} day(s))")
    print(f"  customers  : {len(customers)}")
    print(f"  products   : {len(products)}")
    print(f"  orders     : {total_orders} "
          f"({args.orders_per_day}/day)")
    print(f"  seed       : {args.seed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
