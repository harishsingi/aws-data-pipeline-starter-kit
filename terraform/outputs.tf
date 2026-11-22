# -----------------------------------------------------------------------------
# Outputs: the resource names/ARNs other tooling (CI, glue scripts, notebooks)
# needs to reference this deployment.
# -----------------------------------------------------------------------------

output "raw_bucket_name" {
  description = "Name of the raw (bronze) S3 bucket."
  value       = aws_s3_bucket.this["raw"].id
}

output "raw_bucket_arn" {
  description = "ARN of the raw (bronze) S3 bucket."
  value       = aws_s3_bucket.this["raw"].arn
}

output "curated_bucket_name" {
  description = "Name of the curated (silver) S3 bucket."
  value       = aws_s3_bucket.this["curated"].id
}

output "curated_bucket_arn" {
  description = "ARN of the curated (silver) S3 bucket."
  value       = aws_s3_bucket.this["curated"].arn
}

output "marts_bucket_name" {
  description = "Name of the marts (gold) S3 bucket."
  value       = aws_s3_bucket.this["marts"].id
}

output "marts_bucket_arn" {
  description = "ARN of the marts (gold) S3 bucket."
  value       = aws_s3_bucket.this["marts"].arn
}

output "artifacts_bucket_name" {
  description = "Name of the artifacts S3 bucket (Glue scripts, Lambda zips, Athena results, temp, dbt staging)."
  value       = aws_s3_bucket.this["artifacts"].id
}

output "artifacts_bucket_arn" {
  description = "ARN of the artifacts S3 bucket."
  value       = aws_s3_bucket.this["artifacts"].arn
}

output "glue_database_name" {
  description = "Name of the Glue catalog database."
  value       = aws_glue_catalog_database.this.name
}

output "glue_job_name" {
  description = "Name of the Glue ETL job."
  value       = aws_glue_job.etl.name
}

output "lambda_function_name" {
  description = "Name of the ingest Lambda function."
  value       = aws_lambda_function.ingest.function_name
}

output "athena_workgroup_name" {
  description = "Name of the Athena workgroup."
  value       = aws_athena_workgroup.this.name
}
