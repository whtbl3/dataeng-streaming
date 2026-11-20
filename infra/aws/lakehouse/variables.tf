variable "region" {
  description = "AWS region (spec 009: Singapore, closest to Ho Chi Minh City)"
  type        = string
  default     = "ap-southeast-1"
}

variable "bucket_name" {
  description = "Globally unique S3 bucket for the warehouse and Athena results"
  type        = string
}

variable "budget_email" {
  description = "Where AWS Budgets sends its alerts (set in terraform.tfvars, never committed)"
  type        = string
}

variable "athena_scan_limit_bytes" {
  description = "Athena stops any query that scans more than this (cost guard)"
  type        = number
  default     = 1073741824 # 1 GB
}
