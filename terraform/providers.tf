# -----------------------------------------------------------------------------
# Provider configuration. Region is driven by var.aws_region so the kit can be
# deployed anywhere without editing files. All resources are tagged with the
# kit name and environment by default.
# -----------------------------------------------------------------------------

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "aws-pipeline-starter-kit"
      Environment = var.environment
    }
  }
}
