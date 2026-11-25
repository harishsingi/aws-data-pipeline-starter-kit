#!/usr/bin/env python3
"""Run the whole test suite (stdlib unittest, no AWS/Spark/GX needed).

Usage from the repo root::

    python3 tests/run_tests.py            # all suites
    python3 tests/run_tests.py -v         # verbose
    python3 tests/run_tests.py lambda     # only the Lambda suite
    python3 tests/run_tests.py kit        # only the shared-package + Glue suites

Exit code 0 means every test passed. ``pytest`` can also collect these
tests (``python3 -m pytest tests lambda/tests``) when installed, but it is
not required.
"""

from __future__ import annotations

import os
import sys
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def main(argv: list[str] | None = None) -> int:
    args = list(argv if argv is not None else sys.argv[1:])
    verbose = "-v" in args or "--verbose" in args
    only = next((a for a in args if a in ("lambda", "kit")), None)

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    if only != "kit":
        suite.addTests(
            loader.discover(
                start_dir=os.path.join(REPO_ROOT, "lambda", "tests"),
                top_level_dir=REPO_ROOT,
            )
        )
    if only != "lambda":
        suite.addTests(
            loader.discover(
                start_dir=os.path.join(REPO_ROOT, "tests"),
                pattern="test_*.py",
                top_level_dir=REPO_ROOT,
            )
        )

    runner = unittest.TextTestRunner(verbosity=2 if verbose else 1)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
