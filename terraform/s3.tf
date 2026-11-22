# -----------------------------------------------------------------------------
# S3 data lake storage: raw -> curated -> marts, plus an artifacts bucket for
# Glue scripts, Lambda zips, Athena results, temp files, and dbt staging.
#
# All buckets: versioning ON, SSE-S3 encryption, public access fully blocked.
# force_destroy = false so that `terraform destroy` never silently deletes
# your data lake. If you truly want everything gone, empty the buckets first
# (or set force_destroy = true explicitly at your own risk).
# -----------------------------------------------------------------------------

# --- Account/region data for building ARNs -----------------------------------
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
data "aws_partition" "current" {}

# --- Bucket definitions -------------------------------------------------------
locals {
  bucket_configs = {
    raw = {
      name   = local.raw_bucket_name
      suffix = "Bronze landing zone: validated/ and quarantine/ prefixes"
    }
    curated = {
      name   = local.curated_bucket_name
      suffix = "Silver zone: validated, typed datasets"
    }
    marts = {
      name   = local.marts_bucket_name
      suffix = "Gold zone: analytics-ready marts"
    }
    artifacts = {
      name   = local.artifacts_bucket_name
      suffix = "Pipeline artifacts: glue-scripts/, lambda/, athena-results/, temp/, dbt-staging/"
    }
  }
}

resource "aws_s3_bucket" "this" {
  for_each = local.bucket_configs

  bucket = each.value.name
  # Never delete data silently. terraform destroy will refuse to remove
  # non-empty buckets until you empty them (or flip this deliberately).
  force_destroy = false

  tags = {
    Name = each.value.name
    Zone = each.key
  }
}

resource "aws_s3_bucket_versioning" "this" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256" # SSE-S3
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each = aws_s3_bucket.this

  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# --- Lifecycle rules ----------------------------------------------------------
# Raw data: demote to STANDARD_IA after 30d, GLACIER after 90d.
resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.this["raw"].id

  rule {
    id     = "raw-tiering"
    status = "Enabled"
    filter {} # empty filter = rule applies to all objects in the bucket

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# Curated + marts: demote to STANDARD_IA after 60d.
resource "aws_s3_bucket_lifecycle_configuration" "tiered" {
  for_each = toset(["curated", "marts"])
  bucket   = aws_s3_bucket.this[each.key].id

  rule {
    id     = "${each.key}-tiering"
    status = "Enabled"
    filter {} # empty filter = rule applies to all objects in the bucket

    transition {
      days          = 60
      storage_class = "STANDARD_IA"
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# Artifacts: no tiering needed for short-lived artifacts, but still abort
# incomplete multipart uploads so failed uploads do not accumulate charges.
resource "aws_s3_bucket_lifecycle_configuration" "artifacts" {
  bucket = aws_s3_bucket.this["artifacts"].id

  rule {
    id     = "artifacts-cleanup"
    status = "Enabled"
    filter {} # empty filter = rule applies to all objects in the bucket

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}
