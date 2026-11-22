# -----------------------------------------------------------------------------
# Lambda ingest function: triggered by new objects landing in the raw bucket,
# validates them, and routes to validated/ or quarantine/ prefixes.
# -----------------------------------------------------------------------------

# Package the Lambda source (../lambda/src) as a zip at build time. The zip is
# written under the module's build/ directory (not committed; add build/ to
# your .gitignore).
data "archive_file" "ingest" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda/src"
  output_path = "${path.module}/build/ingest.zip"
}

# Keep the deployment package in the artifacts bucket as well, so the exact
# deployed code version is recoverable later.
resource "aws_s3_object" "lambda_zip" {
  bucket = aws_s3_bucket.this["artifacts"].id
  key    = "lambda/ingest.zip"
  source = data.archive_file.ingest.output_path
  etag   = data.archive_file.ingest.output_md5

  tags = {
    Name = "ingest-lambda-package"
  }
}

resource "aws_lambda_function" "ingest" {
  function_name = local.lambda_name
  role          = aws_iam_role.lambda_exec_role.arn

  handler  = "app.handler"
  runtime  = var.lambda_runtime
  filename = data.archive_file.ingest.output_path

  # Hash change => new version deployed on next apply.
  source_code_hash = data.archive_file.ingest.output_base64sha256

  timeout     = 300
  memory_size = 512

  environment {
    variables = {
      RAW_BUCKET        = aws_s3_bucket.this["raw"].id
      METRICS_NAMESPACE = "DataPipelineKit/Ingestion"
    }
  }

  tags = {
    Name = local.lambda_name
  }

  # The bucket notification (below) registers the Lambda as the target, so the
  # function must exist before the notification is configured.
  depends_on = [aws_iam_role.lambda_exec_role]
}

# Allows S3 to invoke the function. The bucket notification resource
# (aws_s3_bucket_notification below) registers the target with S3, and S3
# checks this permission at invocation time -- the notification will fail to
# fire if this permission is missing, hence the explicit depends_on there.
resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingest.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.this["raw"].arn
}

# Fire the Lambda for every new object under landing/ (incoming raw files).
resource "aws_s3_bucket_notification" "ingest_trigger" {
  bucket = aws_s3_bucket.this["raw"].id

  lambda_function {
    lambda_function_arn = aws_lambda_function.ingest.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "landing/"
  }

  # S3 must be allowed to invoke the function before the notification is
  # registered; otherwise apply fails on the notification.
  depends_on = [aws_lambda_permission.allow_s3]
}
