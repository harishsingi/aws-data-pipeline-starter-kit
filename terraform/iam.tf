# -----------------------------------------------------------------------------
# IAM roles: Glue service role + Lambda execution role, both least-privilege.
# Every ARN is built from data.aws_* sources -- no account IDs or ARNs are
# hard-coded anywhere in this module.
# -----------------------------------------------------------------------------

# --- Glue service role --------------------------------------------------------
resource "aws_iam_role" "glue_service_role" {
  name = local.glue_role_name

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "glue.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Name = local.glue_role_name
  }
}

# Inline least-privilege policy for the Glue ETL job.
resource "aws_iam_role_policy" "glue_service_policy" {
  name = "${local.glue_role_name}-policy"
  role = aws_iam_role.glue_service_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Read/write data + scripts across the four data lake buckets.
      {
        Sid    = "S3BucketLevel"
        Effect = "Allow"
        Action = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [
          aws_s3_bucket.this["raw"].arn,
          aws_s3_bucket.this["curated"].arn,
          aws_s3_bucket.this["marts"].arn,
          aws_s3_bucket.this["artifacts"].arn,
        ]
      },
      {
        Sid    = "S3ObjectLevel"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
        ]
        Resource = [
          "${aws_s3_bucket.this["raw"].arn}/*",
          "${aws_s3_bucket.this["curated"].arn}/*",
          "${aws_s3_bucket.this["marts"].arn}/*",
          "${aws_s3_bucket.this["artifacts"].arn}/*",
        ]
      },
      # Glue catalog: full catalog access, but only this database and its tables.
      {
        Sid    = "GlueCatalog"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:CreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition",
          "glue:BatchCreatePartition",
          "glue:BatchUpdatePartition",
          "glue:BatchDeletePartition",
          "glue:GetUserDefinedFunction",
          "glue:GetUserDefinedFunctions",
        ]
        Resource = [
          "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.this.name}",
          "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.this.name}/*",
        ]
      },
      # CloudWatch Logs: Glue's continuous logging writes here (log groups
      # pre-created in monitoring.tf).
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = [
          "arn:${data.aws_partition.current.partition}:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:${local.glue_output_log_group}:*",
          "arn:${data.aws_partition.current.partition}:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:${local.glue_error_log_group}:*",
        ]
      },
    ]
  })
}

# --- Lambda execution role ----------------------------------------------------
resource "aws_iam_role" "lambda_exec_role" {
  name = local.lambda_role_name

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "lambda.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Name = local.lambda_role_name
  }
}

# Least-privilege policy for the ingest function: read incoming files, write
# validated/quarantined output, list the raw bucket, log, and emit metrics.
resource "aws_iam_role_policy" "lambda_exec_policy" {
  name = "${local.lambda_role_name}-policy"
  role = aws_iam_role.lambda_exec_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "S3RawBucketList"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [aws_s3_bucket.this["raw"].arn]
      },
      {
        Sid      = "S3ReadIncoming"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["${aws_s3_bucket.this["raw"].arn}/landing/*"]
      },
      {
        Sid    = "S3WriteValidatedQuarantine"
        Effect = "Allow"
        Action = ["s3:PutObject"]
        Resource = [
          "${aws_s3_bucket.this["raw"].arn}/validated/*",
          "${aws_s3_bucket.this["raw"].arn}/quarantine/*",
        ]
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = [
          "arn:${data.aws_partition.current.partition}:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${local.lambda_name}:*",
        ]
      },
      {
        Sid      = "CloudWatchMetrics"
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData"]
        Resource = ["*"] # PutMetricData does not support resource-level scoping
      },
    ]
  })
}
