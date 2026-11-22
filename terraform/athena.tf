# -----------------------------------------------------------------------------
# Athena workgroup for ad-hoc queries against curated/marts. Results land in
# the artifacts bucket; workgroup settings are enforced so users cannot bypass
# the encrypted results location or usage limits.
# -----------------------------------------------------------------------------

resource "aws_athena_workgroup" "this" {
  name = local.athena_workgroup

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.this["artifacts"].id}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }

    engine_version {
      selected_engine_version = "AUTO"
    }
  }

  tags = {
    Name = local.athena_workgroup
  }
}
