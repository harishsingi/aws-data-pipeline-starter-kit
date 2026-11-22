# -----------------------------------------------------------------------------
# Terraform core and provider version constraints.
#
# Remote state backend is intentionally COMMENTED OUT. This module ships with
# local state so a buyer can deploy with zero setup. Before using this in a
# team/production setting, uncomment the backend block below and fill in your
# OWN values:
#   - bucket : the S3 bucket YOU create to hold tfstate (must already exist)
#   - key    : the path of the state file inside that bucket
#   - region : the AWS region of the state bucket
# Example values shown below are examples only -- do not reuse them verbatim.
# -----------------------------------------------------------------------------
terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = ">= 2.4"
    }
  }

  # ---------------------------------------------------------------------------
  # Remote state (uncomment and customize for team/production use):
  #
  # backend "s3" {
  #   bucket  = "example-terraform-state-bucket"          # <-- YOUR state bucket (create it first)
  #   key     = "aws-pipeline-starter-kit/dev/terraform.tfstate" # <-- state path for this env
  #   region  = "us-east-1"                               # <-- region of the state bucket
  #   encrypt = true
  # }
  # ---------------------------------------------------------------------------
}
