#!/usr/bin/env bash
#
# Local end-to-end smoke test for the Data Pipeline Starter Kit:
#   1. generate a small sample dataset,
#   2. run the ingestion Lambda unit tests,
#   3. run Great Expectations validation when great-expectations is installed,
#   4. print the next steps (terraform apply, Glue job, dbt build).
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Override with DEMO_DIR=/some/path to keep the demo output elsewhere.
DEMO_DIR="${DEMO_DIR:-/tmp/kit-demo}"
SAMPLE_OUT="$DEMO_DIR/sample_data"

echo "==> [1/3] Generating sample data (2 days x 50 orders) ..."
python3 scripts/generate_sample_data.py \
  --days 2 \
  --orders-per-day 50 \
  --out "$SAMPLE_OUT"

echo
echo "==> [2/3] Running ingestion Lambda unit tests ..."
python3 -m unittest discover -s lambda/tests

echo
echo "==> [3/3] Great Expectations validation ..."
if python3 -c "import great_expectations" >/dev/null 2>&1; then
  if [[ -f "data-quality/great_expectations/validate_orders.py" ]]; then
    python3 data-quality/great_expectations/validate_orders.py \
      --data-dir "$SAMPLE_OUT/raw/landing/orders"
  else
    echo "great-expectations is installed, but"
    echo "data-quality/great_expectations/validate_orders.py is not present yet."
    echo "Skipping GX validation."
  fi
else
  echo "great-expectations is not installed - skipping GX validation."
  echo "Install it with: pip install -r data-quality/great_expectations/requirements.txt"
fi

echo
echo "==> Demo complete. Sample data is in: $SAMPLE_OUT"
echo "Next steps:"
echo "  1. Deploy the infrastructure:"
echo "       cd terraform && terraform init && terraform apply"
echo "  2. Upload the sample orders to the raw bucket landing prefix"
echo "     (the ingestion Lambda fires automatically on each new file):"
echo "       aws s3 sync $SAMPLE_OUT/raw/landing/orders s3://<raw-bucket>/landing/orders"
echo "     Check validated/ and quarantine/ prefixes for the routed output."
echo "  3. Run the Glue ETL job to build the curated layer (see glue/etl_job.py)."
echo "  4. Build and test the marts:"
echo "       cd dbt && dbt build"
