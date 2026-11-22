# -----------------------------------------------------------------------------
# Input variables for the AWS Data Pipeline Starter Kit.
# -----------------------------------------------------------------------------

variable "project_name" {
  description = "Project name used as a prefix for all resource names. Must be lowercase, letters/numbers/hyphens only (S3-safe)."
  type        = string
  default     = "data-pipeline-kit"

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]*$", var.project_name))
    error_message = "project_name must be lowercase alphanumeric with hyphens only (S3 bucket safe), and start with a letter or number."
  }
}

variable "environment" {
  description = "Deployment environment name (e.g. dev, staging, prod). Used in resource names and tags."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "aws_region" {
  description = "AWS region to deploy the pipeline into."
  type        = string
  default     = "us-east-1"
}

variable "glue_worker_type" {
  description = "Glue worker type for the ETL job (G.1X, G.2X, G.4X, G.8X)."
  type        = string
  default     = "G.1X"

  validation {
    condition     = contains(["G.1X", "G.2X", "G.4X", "G.8X"], var.glue_worker_type)
    error_message = "glue_worker_type must be one of: G.1X, G.2X, G.4X, G.8X."
  }
}

variable "glue_num_workers" {
  description = "Number of Glue workers for the ETL job."
  type        = number
  default     = 2

  validation {
    condition     = var.glue_num_workers >= 1 && var.glue_num_workers <= 100
    error_message = "glue_num_workers must be between 1 and 100."
  }
}

variable "lambda_runtime" {
  description = "Lambda runtime for the ingest function."
  type        = string
  default     = "python3.12"

  validation {
    condition     = can(regex("^python3\\.(1[0-2]|[9])$", var.lambda_runtime))
    error_message = "lambda_runtime must be a supported python3 runtime (e.g. python3.9 through python3.12)."
  }
}

variable "notification_email" {
  description = "Email address for CloudWatch alarm notifications. Leave empty to skip SNS topic and email subscription."
  type        = string
  default     = ""

  validation {
    condition     = var.notification_email == "" || can(regex("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", var.notification_email))
    error_message = "notification_email must be empty or a valid email address."
  }
}
