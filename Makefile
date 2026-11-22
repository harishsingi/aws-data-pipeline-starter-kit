# Makefile — common operations for the AWS Data Pipeline Starter Kit
# Run `make help` to list targets.

# Today's date in yyyy-MM-dd, used as the default RUN_DATE for Glue runs
RUN_DATE ?= $(shell date +%F)

.PHONY: help sample-data test-lambda validate-gx tf-init tf-plan tf-apply tf-destroy glue-run dbt-local dbt-prod clean

# Default target: show help
help:
	@echo "Available targets:"
	@echo "  sample-data   Generate synthetic sample data (~1 min)"
	@echo "  test-lambda   Run Lambda unit tests"
	@echo "  validate-gx   Run Great Expectations suite (skips if not installed)"
	@echo "  tf-init       terraform init"
	@echo "  tf-plan       terraform plan"
	@echo "  tf-apply      terraform apply (provisions AWS infra, ~10-20 min)"
	@echo "  tf-destroy    terraform destroy (tears down AWS infra)"
	@echo "  glue-run      Start the Glue ETL job (RUN_DATE=$(RUN_DATE), override: make glue-run RUN_DATE=2026-09-24)"
	@echo "  dbt-local     dbt build on local duckdb target (~2 min, no AWS)"
	@echo "  dbt-prod      dbt build on prod target (Athena)"
	@echo "  clean         Remove generated artifacts (__pycache__, dbt target/, logs)"

# Generate synthetic orders/customers/products for end-to-end testing
sample-data:
	python scripts/generate_sample_data.py

# Lambda unit tests (unittest discover pattern, same as CI)
test-lambda:
	python -m unittest discover -s lambda/tests -v

# Great Expectations validation of sample landing data; no-op with a message if GX isn't installed
validate-gx:
	@if python -c "import great_expectations" 2>/dev/null; then \
		python data-quality/great_expectations/validate_orders.py --input "sample_data/raw/landing/orders/*/*.jsonl"; \
	else \
		echo "great_expectations not installed — skipping (pip install -r data-quality/great_expectations/requirements.txt to enable)"; \
	fi

# Terraform lifecycle
tf-init:
	terraform -chdir=terraform init

tf-plan:
	terraform -chdir=terraform plan

tf-apply:
	terraform -chdir=terraform apply

tf-destroy:
	terraform -chdir=terraform destroy

# Start the Glue ETL job for a given run date (defaults to today)
glue-run:
	aws glue start-job-run --job-name data-pipeline-kit-dev-etl-job \
		--arguments '{"--RUN_DATE": "$(RUN_DATE)"}'

# dbt: local duckdb target for fast iteration without AWS
dbt-local:
	cd dbt && dbt build --target local

# dbt: prod target against Athena
dbt-prod:
	cd dbt && dbt build --target prod

# Remove generated artifacts (keeps source and sample data)
clean:
	find . -name '__pycache__' -type d -prune -exec rm -rf {} + 2>/dev/null || true
	rm -rf dbt/target dbt/logs dbt/dbt_packages
