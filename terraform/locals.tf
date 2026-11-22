# -----------------------------------------------------------------------------
# Derived resource names. These are the naming contract used by every other
# file in this module (and by the glue/lambda components that reference them).
# -----------------------------------------------------------------------------

locals {
  # S3 buckets: "<project_name>-<environment>-<stage>"
  raw_bucket_name       = "${var.project_name}-${var.environment}-raw"
  curated_bucket_name   = "${var.project_name}-${var.environment}-curated"
  marts_bucket_name     = "${var.project_name}-${var.environment}-marts"
  artifacts_bucket_name = "${var.project_name}-${var.environment}-artifacts"

  # Glue database name: hyphens replaced with underscores because Glue
  # identifiers do not allow hyphens (e.g. data_pipeline_kit_dev).
  glue_database_name = replace("${var.project_name}_${var.environment}", "-", "_")

  # Compute / function names
  glue_job_name    = "${var.project_name}-${var.environment}-etl-job"
  lambda_name      = "${var.project_name}-${var.environment}-ingest"
  athena_workgroup = "${var.project_name}-${var.environment}-wg"

  # IAM role names (must be <= 64 chars for IAM)
  glue_role_name   = "${var.project_name}-${var.environment}-glue-role"
  lambda_role_name = "${var.project_name}-${var.environment}-lambda-role"

  # Glue writes its continuous logging to these log groups; we pre-create them
  # in monitoring.tf with a 30-day retention.
  glue_output_log_group = "/aws-glue/jobs/output"
  glue_error_log_group  = "/aws-glue/jobs/error"
}
