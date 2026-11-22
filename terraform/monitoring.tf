# -----------------------------------------------------------------------------
# Monitoring: CloudWatch log groups, Lambda alarms, and optional email alerts.
#
# Glue continuous logging writes to /aws-glue/jobs/output and
# /aws-glue/jobs/error; the groups are pre-created here with a 30-day
# retention so we own the retention policy instead of accepting the default
# (never expire).
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${local.lambda_name}"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "glue_output" {
  name              = local.glue_output_log_group # /aws-glue/jobs/output
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "glue_error" {
  name              = local.glue_error_log_group # /aws-glue/jobs/error
  retention_in_days = 30
}

# --- Optional SNS notifications ------------------------------------------------
# Created only when var.notification_email is non-empty. SNS requires the
# recipient to confirm the subscription email before alerts flow.
resource "aws_sns_topic" "alerts" {
  count = var.notification_email != "" ? 1 : 0
  name  = "${var.project_name}-${var.environment}-alerts"
}

resource "aws_sns_topic_subscription" "alerts_email" {
  count     = var.notification_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.alerts[0].arn
  protocol  = "email"
  endpoint  = var.notification_email
}

locals {
  # Empty list when no email configured -> alarms have no actions (still visible
  # in the console, evaluated, and cheap).
  alarm_actions = var.notification_email != "" ? [aws_sns_topic.alerts[0].arn] : []
}

# --- Lambda alarms -------------------------------------------------------------
resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "${local.lambda_name}-errors"
  alarm_description   = "Ingest Lambda error count >= 1 in 5 minutes"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.ingest.function_name
  }

  alarm_actions = local.alarm_actions
}

resource "aws_cloudwatch_metric_alarm" "lambda_throttles" {
  alarm_name          = "${local.lambda_name}-throttles"
  alarm_description   = "Ingest Lambda throttle count >= 1 in 5 minutes"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "Throttles"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.ingest.function_name
  }

  alarm_actions = local.alarm_actions
}
