"""Record validation for order records (pure functions, no I/O).

This is the canonical copy of the validation rules. The Lambda
(``lambda/src/validation.py``) carries a mirrored copy because its
deployment zip is self-contained; ``tests/test_mirrors.py`` asserts the two
agree on a shared corpus, so a rule change here must be mirrored there.

Rules (error codes in parentheses):

* every required field present and non-``None`` (``missing_field:<name>``)
* identifier fields are non-empty strings
  (``invalid_order_id`` / ``invalid_customer_id`` / ``invalid_product_id``)
* ``order_ts`` parses as ISO8601, trailing ``Z`` tolerated
  (``invalid_order_ts``)
* ``quantity`` is an integer (booleans rejected; integral floats tolerated),
  1..1000 (``invalid_quantity``)
* ``unit_price`` is a number (booleans rejected), ``>= 0``
  (``invalid_unit_price``)
* ``discount`` is a number (booleans rejected), ``>= 0`` and
  ``<= unit_price`` (``invalid_discount``)
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import PurePosixPath

from pipeline_kit.config import (
    QUANTITY_MAX,
    QUANTITY_MIN,
    REQUIRED_FIELDS,
)

_DT_SEGMENT_RE = re.compile(r"^dt=(\d{4}-\d{2}-\d{2})$")
_IDENTIFIER_FIELDS = ("order_id", "customer_id", "product_id")


def parse_order_timestamp(value: object) -> datetime | None:
    """Parse an ISO8601 timestamp, tolerating a trailing ``Z``.

    Returns ``None`` when the value does not parse (callers report
    ``invalid_order_ts``); never raises.
    """
    if value is None:
        return None
    text = value if isinstance(value, str) else str(value)
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except (ValueError, TypeError):
        return None


def validate_record(rec: dict) -> list[str]:
    """Validate a single order record.

    Pure function: no I/O and no side effects.

    Args:
        rec: The decoded JSON object for one order.

    Returns:
        A list of error codes; an empty list means the record is valid.
        All applicable errors are reported, not just the first.
    """
    errors: list[str] = []

    if not isinstance(rec, dict):
        return ["not_an_object"]

    # 1. all required fields present (None counts as missing) ----------------
    for field in REQUIRED_FIELDS:
        if field not in rec or rec[field] is None:
            errors.append(f"missing_field:{field}")

    # 2. identifier fields are non-empty strings -----------------------------
    for field in _IDENTIFIER_FIELDS:
        value = rec.get(field)
        if value is None:
            continue  # already reported as missing above
        if not isinstance(value, str) or not value.strip():
            errors.append(f"invalid_{field}")

    # 3. order_ts parses as ISO8601 ------------------------------------------
    ts = rec.get("order_ts")
    if ts is not None and parse_order_timestamp(ts) is None:
        errors.append("invalid_order_ts")

    # 4. quantity: int (bool excluded), QUANTITY_MIN..QUANTITY_MAX ------------
    quantity = rec.get("quantity")
    quantity_ok = False
    if quantity is not None and not isinstance(quantity, bool):
        if isinstance(quantity, int):
            quantity_ok = QUANTITY_MIN <= quantity <= QUANTITY_MAX
        elif isinstance(quantity, float) and quantity.is_integer():
            # tolerate integral floats (e.g. 3.0) from loose producers
            quantity_ok = QUANTITY_MIN <= int(quantity) <= QUANTITY_MAX
    if quantity is not None and not quantity_ok:
        errors.append("invalid_quantity")

    # 5. unit_price: number (bool excluded), >= 0 -----------------------------
    unit_price = rec.get("unit_price")
    unit_price_value: float | None = None
    if (
        unit_price is not None
        and not isinstance(unit_price, bool)
        and isinstance(unit_price, (int, float))
        and unit_price >= 0
    ):
        unit_price_value = float(unit_price)
    elif unit_price is not None:
        errors.append("invalid_unit_price")

    # 6. discount: number (bool excluded), 0 <= discount <= unit_price --------
    discount = rec.get("discount")
    if discount is not None:
        discount_ok = (
            not isinstance(discount, bool)
            and isinstance(discount, (int, float))
            and discount >= 0
            and (unit_price_value is None or discount <= unit_price_value)
        )
        if not discount_ok:
            errors.append("invalid_discount")

    return errors


def derive_dt_from_path(path: str) -> str | None:
    """Derive the ``dt=YYYY-MM-DD`` partition value from a file path.

    Landing files carry no ``dt`` column; the partition is part of the path
    (``.../landing/orders/dt=2026-09-25/orders.jsonl``). Returns the
    ``YYYY-MM-DD`` string, or ``None`` when no ``dt=`` segment is present.

    >>> derive_dt_from_path("landing/orders/dt=2026-09-25/orders.jsonl")
    '2026-09-25'
    >>> derive_dt_from_path("orders.jsonl") is None
    True
    """
    for segment in PurePosixPath(path).parts:
        match = _DT_SEGMENT_RE.match(segment)
        if match:
            return match.group(1)
    return None


def check_required_columns(columns: list[str] | set[str], required: list[str]) -> list[str]:
    """Return the required columns missing from ``columns`` (empty = OK).

    Pure helper shared by the Great Expectations loader so the column
    contract is checked the same way everywhere.
    """
    present = set(columns)
    return [c for c in required if c not in present]
