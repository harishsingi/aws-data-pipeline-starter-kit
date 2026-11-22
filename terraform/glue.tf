# -----------------------------------------------------------------------------
# AWS Glue: catalog database + ETL job.
# -----------------------------------------------------------------------------

resource "aws_glue_catalog_database" "this" {
  name = local.glue_database_name

  description = "Data Pipeline Starter Kit catalog (${var.environment})"
}

# Upload the ETL script to the artifacts bucket. etag = filemd5 forces a new
# object whenever the script content changes, which re-triggers the Glue job
# resource update (via script_location etag below) on the next apply.
resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.this["artifacts"].id
  key    = "glue-scripts/etl_job.py"
  source = "${path.module}/../glue/etl_job.py"
  etag   = filemd5("${path.module}/../glue/etl_job.py")

  tags = {
    Name = "etl-job-script"
  }
}

resource "aws_glue_job" "etl" {
  name     = local.glue_job_name
  role_arn = aws_iam_role.glue_service_role.arn

  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_num_workers

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.this["artifacts"].id}/${aws_s3_object.glue_script.key}"
  }

  default_arguments = {
    "--TempDir"                          = "s3://${aws_s3_bucket.this["artifacts"].id}/temp/"
    "--DATABASE"                         = aws_glue_catalog_database.this.name
    "--RAW_BUCKET"                       = aws_s3_bucket.this["raw"].id
    "--CURATED_BUCKET"                   = aws_s3_bucket.this["curated"].id
    "--MARTS_BUCKET"                     = aws_s3_bucket.this["marts"].id
    "--enable-metrics"                   = ""
    "--enable-continuous-cloudwatch-log" = "true"
    # The job overwrites whole date partitions (dt=...), so re-runs are
    # idempotent by construction; job bookmarks are disabled deliberately.
    "--job-bookmark-option" = "job-bookmark-disable"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  timeout     = 60
  max_retries = 1

  tags = {
    Name = local.glue_job_name
  }
}
