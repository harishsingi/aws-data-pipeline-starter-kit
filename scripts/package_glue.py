#!/usr/bin/env python3
"""Package the Glue helper modules (glue/src/etl) as a zip for --extra-py-files.

Excludes ``__pycache__`` and ``*.pyc`` so a local compile check never leaks
bytecode into the deployed zip. Terraform builds the same zip itself via
``archive_file``; this script is for manual uploads and local inspection.

Usage:
    python3 scripts/package_glue.py [--src glue/src] [--out dist/etl_package.zip]
"""

import argparse
import logging
import zipfile
from pathlib import Path

logger = logging.getLogger("package_glue")

EXCLUDED_DIRS = {"__pycache__"}


def package(src: Path, out: Path) -> list[str]:
    """Zip ``src/etl`` (at the zip root) into ``out``; return member names."""
    etl_dir = src / "etl"
    if not etl_dir.is_dir():
        raise SystemExit(f"ERROR: no etl package found under {src}")
    out.parent.mkdir(parents=True, exist_ok=True)
    members: list[str] = []
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in sorted(etl_dir.rglob("*")):
            if path.is_dir():
                continue
            if path.suffix == ".pyc" or any(p in EXCLUDED_DIRS for p in path.parts):
                continue
            arcname = path.relative_to(src).as_posix()
            zf.write(path, arcname)
            members.append(arcname)
    return members


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--src", default="glue/src")
    parser.add_argument("--out", default="dist/etl_package.zip")
    args = parser.parse_args(argv)
    members = package(Path(args.src), Path(args.out))
    logger.info("Wrote %s (%d files):", args.out, len(members))
    for member in members:
        logger.info("  %s", member)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
