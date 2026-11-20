# AWS lakehouse for spec 009 (ADR 0015): S3 + Glue Data Catalog + Athena, nothing else.
# Created and removed by `make aws-lake-up` / `make aws-lake-down`, only with the owner's OK.
terraform {
  required_version = ">= 1.9"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      project    = "delivery-control-tower"
      feature    = "spec-009-lakehouse"
      managed_by = "terraform"
    }
  }
}
