"""Pure transform helpers mirroring the Glue job's Spark logic.

The Glue job (``glue/src/etl/``) implements these same rules with Spark
SQL functions (``sha2``, ``date_format``, decimal arithmetic). These stdlib
equivalents exist so the *math* can be unit-tested locally without Spark,
and so the key-derivation rules are documented in one place:

* ``customer_sk`` = ``sha256_hex(customer_id || effective_from)``
  (Spark: ``sha2(concat_ws("||", customer_id, effective_from), 256)``)
* ``product_sk`` = ``sha256_hex(product_id)``
  (Spark: ``sha2(product_id, 256)``)
* ``date_sk`` = ``YYYYMMDD`` as an integer
* ``quarter`` = ``(month - 1) // 3 + 1``
* ``line_total`` = ``quantity * (unit_price - discount)``, exact decimal
  arithmetic, quantised to 2 places (Spark: ``DECIMAL(12,2)`` cast)
"""

from __future__ import annotations

import hashlib
from datetime import date
from decimal import Decimal, ROUND_HALF_UP

from pipeline_kit.config import EFFECTIVE_TO_OPEN, ORPHAN_FAILURE_THRESHOLD

_TWO_PLACES = Decimal("0.01")


def customer_surrogate_key(customer_id: str, effective_from: str | date) -> str:
    """Deterministic SCD2 surrogate key for a customer version.

    Args:
        customer_id: Business key, e.g. ``"C0001"``.
        effective_from: Version start date (``YYYY-MM-DD`` or a date).

    Raises:
        ValueError: If ``customer_id`` is blank.
    """
    if not customer_id or not customer_id.strip():
        raise ValueError("customer_id must not be blank.")
    start = effective_from.isoformat() if isinstance(effective_from, date) else effective_from
    return hashlib.sha256(f"{customer_id}||{start}".encode("utf-8")).hexdigest()


def product_surrogate_key(product_id: str) -> str:
    """Deterministic SCD1 surrogate key for a product.

    Raises:
        ValueError: If ``product_id`` is blank.
    """
    if not product_id or not product_id.strip():
        raise ValueError("product_id must not be blank.")
    return hashlib.sha256(product_id.encode("utf-8")).hexdigest()


def date_sk(day: date) -> int:
    """Calendar surrogate key: ``YYYYMMDD`` as an integer (e.g. 20260925)."""
    return day.year * 10_000 + day.month * 100 + day.day


def quarter_from_month(month: int) -> int:
    """Calendar quarter for a month number (1..12).

    Raises:
        ValueError: If ``month`` is outside 1..12.
    """
    if not 1 <= month <= 12:
        raise ValueError(f"month must be 1..12, got {month}.")
    return (month - 1) // 3 + 1


def line_total(quantity: int, unit_price: Decimal, discount: Decimal) -> Decimal:
    """Order line total: ``quantity * (unit_price - discount)``.

    Uses :class:`~decimal.Decimal` so the result is exact to the cent,
    matching the Glue job's ``DECIMAL(12,2)`` cast. A negative result is
    returned as-is; the data-quality gate (not this function) rejects it.

    Raises:
        ValueError: If ``quantity`` is negative.
    """
    if quantity < 0:
        raise ValueError(f"quantity must be >= 0, got {quantity}.")
    total = (Decimal(quantity) * (unit_price - discount)).quantize(
        _TWO_PLACES, rounding=ROUND_HALF_UP
    )
    return total


def orphan_rate_exceeds_threshold(
    orphan_rows: int, total_rows: int, threshold: float = ORPHAN_FAILURE_THRESHOLD
) -> bool:
    """True when orphaned fact rows exceed ``threshold`` of the total.

    Mirrors the Glue job's fail-fast gate. Zero total rows never exceeds
    (an empty fact is caught by the separate row-count gate instead).
    """
    if total_rows <= 0:
        return False
    if orphan_rows < 0:
        raise ValueError(f"orphan_rows must be >= 0, got {orphan_rows}.")
    return (orphan_rows / total_rows) > threshold


def is_open_ended(effective_to: str) -> bool:
    """True when an SCD2 ``effective_to`` is the open-ended sentinel."""
    return effective_to == EFFECTIVE_TO_OPEN
