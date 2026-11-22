#!/usr/bin/env python3
"""validate_orders.py — Great Expectations contract check for raw order files.

Validates landing-zone order files (JSONL or Parquet) against the
`orders_suite` expectation suite BEFORE they are written to the silver
layer by the Glue ingestion jobs. Use it:

* locally, while developing:  python validate_orders.py --input "sample_data/raw/landing/orders/*/*.jsonl"
* in CI / pre-Glue:            fail the pipeline when the file contract breaks

Exits 0 when every expectation passes, 1 when any expectation fails
(or when no input files are found / loading fails).

Requires: pip install -r requirements.txt
"""

from __future__ import annotations

import argparse
import glob
import sys
from pathlib import Path

import pandas as pd

import great_expectations as gx
from great_expectations import expectations as gxe

SUITE_NAME = "orders_suite"

# Column contract for raw order landing files. Keep in sync with the
# silver_orders schema consumed by dbt (dbt/models/sources.yml).
REQUIRED_COLUMNS = [
    "order_id",
    "order_ts",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "discount",
    "dt",
]


def load_input(pattern: str) -> pd.DataFrame:
    """Load every file matching the glob into a single DataFrame."""
    paths = sorted(glob.glob(pattern, recursive=True))
    if not paths:
        raise SystemExit(f"ERROR: no files matched input pattern: {pattern}")

    frames: list[pd.DataFrame] = []
    for raw in paths:
        suffix = Path(raw).suffix.lower()
        if suffix in {".jsonl", ".json"}:
            # Landing files are newline-delimited JSON (one order per line).
            frames.append(pd.read_json(raw, lines=True))
        elif suffix == ".parquet":
            frames.append(pd.read_parquet(raw))
        else:
            raise SystemExit(
                f"ERROR: unsupported file type for {raw!r}; "
                "expected .jsonl, .json or .parquet"
            )

    df = pd.concat(frames, ignore_index=True)
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise SystemExit(f"ERROR: input is missing required columns: {missing}")
    return df


def build_suite() -> gx.ExpectationSuite:
    """Define the orders file contract as an ExpectationSuite.

    Requires an active GX context (call ``gx.get_context()`` first):
    ``suite.add_expectation`` checks the expectations store, which only
    exists once a context is active.
    """
    suite = gx.ExpectationSuite(name=SUITE_NAME)

    # Key columns must always be populated — nulls here break silver writes
    # and every downstream dbt join.
    for column in ("order_id", "customer_id", "product_id", "order_ts"):
        suite.add_expectation(
            gxe.ExpectColumnValuesToNotBeNull(column=column)
        )

    # order_id is the business key; duplicates would corrupt order counts.
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="order_id"))

    # Sane business ranges: quantity is a small positive integer, unit_price
    # is bounded well above any realistic product price.
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(
            column="quantity", min_value=1, max_value=1000
        )
    )
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(
            column="unit_price", min_value=0, max_value=100000
        )
    )

    # Order IDs must follow the upstream format (e.g. ORD-1001).
    suite.add_expectation(
        gxe.ExpectColumnValuesToMatchRegex(column="order_id", regex=r"^ORD-")
    )

    return suite


def validate(
    context: gx.EphemeralDataContext, df: pd.DataFrame, suite: gx.ExpectationSuite
):
    """Run the suite against the DataFrame via the GX 1.x fluent API."""
    datasource = context.data_sources.add_pandas("orders_datasource")
    asset = datasource.add_dataframe_asset(name="orders_asset")
    batch_definition = asset.add_batch_definition_whole_dataframe(
        "orders_batch_definition"
    )
    # Whole-dataframe batch: the DataFrame itself is the batch parameter.
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    return batch.validate(suite)


def print_summary(validation_result) -> bool:
    """Print a human-readable summary. Returns True when all passed."""
    results = validation_result.results
    evaluated = len(results)
    passed = [r for r in results if r.success]
    failed = [r for r in results if not r.success]

    print(f"Suite: {SUITE_NAME}")
    print(f"Evaluated expectations : {evaluated}")
    print(f"Successful             : {len(passed)}")
    print(f"Failed                 : {len(failed)}")

    if failed:
        print("\nFailed expectations:")
        for r in failed:
            expectation_type = r.expectation_config.type
            column = r.expectation_config.kwargs.get("column", "?")
            print(f"  - {expectation_type} (column: {column})")
        return False
    return True


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate raw order landing files against the orders_suite "
        "Great Expectations contract."
    )
    parser.add_argument(
        "--input",
        required=True,
        help='Glob for input files, e.g. "sample_data/raw/landing/orders/*/*.jsonl" '
        "or a single .parquet path.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    try:
        df = load_input(args.input)
    except SystemExit as exc:
        print(exc, file=sys.stderr)
        return 1

    print(f"Loaded {len(df)} rows from {args.input!r}")

    # Ephemeral context: no great_expectations.yml project on disk, no
    # Data Docs — just validate and report. Perfect for CI/pre-Glue checks.
    # NOTE: the context must be created BEFORE build_suite(), because
    # suite.add_expectation() requires an active context.
    context = gx.get_context(mode="ephemeral")
    suite = build_suite()
    result = validate(context, df, suite)
    ok = print_summary(result)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
