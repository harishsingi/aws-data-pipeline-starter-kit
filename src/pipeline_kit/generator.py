"""Deterministic synthetic sample-data generation (stdlib only).

Writes reference data and per-day order files:

  ``<out>/raw/reference/customers/dt=<today>/customers.json``
  ``<out>/raw/reference/products/dt=<today>/products.json``
  ``<out>/raw/landing/orders/dt=<date>/orders.jsonl`` (one file per day)

All randomness comes from ``random.Random(seed)``, so the output is fully
reproducible for a given seed. All data is synthetic: invented names from
fixed pools, ``example.com`` email addresses, generic product names. No
real customer, company or location data is used anywhere.

The thin CLI wrapper is ``scripts/generate_sample_data.py``; this module
holds the logic so it can be unit-tested without a subprocess.
"""

from __future__ import annotations

import json
import logging
import os
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from pipeline_kit.errors import ConfigurationError
from pipeline_kit.metrics import RowCounts

logger = logging.getLogger(__name__)

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

DEFAULT_DAYS = 7
DEFAULT_ORDERS_PER_DAY = 500
DEFAULT_CUSTOMERS = 200
DEFAULT_PRODUCTS = 50
DEFAULT_SEED = 42


@dataclass(frozen=True)
class GeneratorOptions:
    """Validated options for :func:`generate_dataset`."""

    days: int = DEFAULT_DAYS
    orders_per_day: int = DEFAULT_ORDERS_PER_DAY
    customers: int = DEFAULT_CUSTOMERS
    products: int = DEFAULT_PRODUCTS
    seed: int = DEFAULT_SEED
    out: str | Path = "sample_data"

    def __post_init__(self) -> None:
        for name in ("days", "orders_per_day", "customers", "products"):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise ConfigurationError(f"--{name.replace('_', '-')} must be an integer >= 1, got {value!r}.")
        if not str(self.out).strip():
            raise ConfigurationError("--out must not be blank.")


@dataclass(frozen=True)
class DatasetSummary:
    """What :func:`generate_dataset` wrote, for logging and tests."""

    out_dir: Path
    first_day: date
    last_day: date
    days: int
    customers: int
    products: int
    orders: int
    seed: int


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------


def generate_customers(rng: random.Random, count: int) -> list[dict]:
    """Generate customer reference records with unique synthetic emails.

    Raises:
        ConfigurationError: If ``count`` is less than 1.
    """
    if count < 1:
        raise ConfigurationError(f"customer count must be >= 1, got {count}.")
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
    """Generate product reference records, cycling through categories.

    Raises:
        ConfigurationError: If ``count`` is less than 1.
    """
    if count < 1:
        raise ConfigurationError(f"product count must be >= 1, got {count}.")
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
    """Generate one day of order records with timestamps spread through it.

    Discounts never exceed the unit price, quantities are 1..5, and every
    order references an existing generated customer and product, so the
    output passes the Lambda validator by construction.

    Raises:
        ConfigurationError: If ``orders_per_day`` < 1 or either reference
            list is empty.
    """
    if orders_per_day < 1:
        raise ConfigurationError(f"orders_per_day must be >= 1, got {orders_per_day}.")
    if not customers:
        raise ConfigurationError("cannot generate orders without customers.")
    if not products:
        raise ConfigurationError("cannot generate orders without products.")
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


def write_json(path: str | Path, records: list[dict]) -> None:
    """Write records as pretty JSON, creating parent directories."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(records, fh, indent=2)
        fh.write("\n")


def write_jsonl(path: str | Path, records: list[dict]) -> None:
    """Write records as JSON Lines, creating parent directories."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record) + "\n")


def generate_dataset(options: GeneratorOptions, *, today: date | None = None) -> DatasetSummary:
    """Generate the full sample dataset under ``options.out``.

    Args:
        options: Validated generator options.
        today: Reference date (defaults to today UTC); injectable for tests.

    Returns:
        A :class:`DatasetSummary` describing what was written.
    """
    rng = random.Random(options.seed)
    today = today or datetime.now(timezone.utc).date()

    customers = generate_customers(rng, options.customers)
    products = generate_products(rng, options.products)

    out = Path(options.out)
    ref_base = out / "raw" / "reference"
    write_json(
        ref_base / "customers" / f"dt={today.isoformat()}" / "customers.json",
        customers,
    )
    write_json(
        ref_base / "products" / f"dt={today.isoformat()}" / "products.json",
        products,
    )

    first_day = today - timedelta(days=options.days - 1)
    seq = 1
    total_orders = 0
    for offset in range(options.days):
        day = first_day + timedelta(days=offset)
        orders = generate_orders(
            rng, customers, products, day, options.orders_per_day, start_seq=seq
        )
        seq += len(orders)
        total_orders += len(orders)
        write_jsonl(
            out / "raw" / "landing" / "orders" / f"dt={day.isoformat()}" / "orders.jsonl",
            orders,
        )

    RowCounts(
        rows_in=total_orders, rows_out=total_orders,
    ).log_summary(logger, "generate_dataset")
    logger.info(
        "Sample data generated: out=%s date_range=%s..%s customers=%d products=%d "
        "orders=%d seed=%d",
        os.path.abspath(out),
        first_day.isoformat(),
        today.isoformat(),
        len(customers),
        len(products),
        total_orders,
        options.seed,
    )
    return DatasetSummary(
        out_dir=out,
        first_day=first_day,
        last_day=today,
        days=options.days,
        customers=len(customers),
        products=len(products),
        orders=total_orders,
        seed=options.seed,
    )
