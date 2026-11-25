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
# The script is a thin driver; its helper modules live in ../glue/src/etl
# and are packaged as --extra-py-files below.
resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.this["artifacts"].id
  key    = "glue-scripts/etl_job.py"
  source = "${path.module}/../glue/etl_job.py"
  etag   = filemd5("${path.module}/../glue/etl_job.py")

  tags = {
    Name = "etl-job-script"
  }
}

# Package the ETL helper modules (../glue/src) as a zip at build time. The
# zip is written under the module's build/ directory (not committed; add
# build/ to your .gitignore). Glue extracts --extra-py-files zips onto the
# Python path, so `etl_job.py` can `from etl.config import ...`.
# NOTE: run `make clean` before `terraform apply` so no local __pycache__
# is swept into the zip.
data "archive_file" "etl_package" {
  type        = "zip"
  source_dir  = "${path.module}/../glue/src"
  output_path = "${path.module}/build/etl_package.zip"
}

resource "aws_s3_object" "etl_package" {
  bucket = aws_s3_bucket.this["artifacts"].id
  key    = "glue-scripts/etl_package.zip"
  source = data.archive_file.etl_package.output_path
  etag   = data.archive_file.etl_package.output_md5

  tags = {
    Name = "etl-package"
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
    # Helper modules for the thin driver script (glue/src/etl/*, packaged
    # as a zip above). Without this the `from etl...` imports fail.
    "--extra-py-files" = "s3://${aws_s3_bucket.this["artifacts"].id}/${aws_s3_object.etl_package.key}"
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
