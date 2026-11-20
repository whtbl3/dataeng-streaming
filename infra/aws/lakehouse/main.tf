data "aws_caller_identity" "me" {}
data "aws_partition" "this" {}

locals {
  account   = data.aws_caller_identity.me.account_id
  bucket    = aws_s3_bucket.lake.bucket
  databases = ["bronze", "silver", "gold", "gold_staging", "ops"]
}

# ---- storage ---------------------------------------------------------------------------------
resource "aws_s3_bucket" "lake" {
  bucket        = var.bucket_name
  force_destroy = true # `aws-lake-down` empties it: nothing billable is left behind
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "athena-results"
    status = "Enabled"
    filter {
      prefix = "athena-results/"
    }
    expiration {
      days = 1
    }
  }
}

# ---- catalog ---------------------------------------------------------------------------------
resource "aws_glue_catalog_database" "db" {
  for_each     = toset(local.databases)
  name         = each.key
  location_uri = "s3://${local.bucket}/warehouse/${each.key}.db"
}

# ---- query -----------------------------------------------------------------------------------
resource "aws_athena_workgroup" "ct" {
  name          = "control-tower"
  force_destroy = true
  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false
    bytes_scanned_cutoff_per_query     = var.athena_scan_limit_bytes
    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
    result_configuration {
      output_location = "s3://${local.bucket}/athena-results/"
    }
  }
}

# ---- access: one demo role, scoped to this bucket, these databases and the workgroup --------
data "aws_iam_policy_document" "assume_from_owner" {
  statement {
    actions = ["sts:AssumeRole", "sts:TagSession"]
    principals {
      type        = "AWS"
      identifiers = ["arn:${data.aws_partition.this.partition}:iam::${local.account}:root"]
    }
  }
}

resource "aws_iam_role" "demo" {
  name                 = "control-tower-lakehouse-demo"
  assume_role_policy   = data.aws_iam_policy_document.assume_from_owner.json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "demo" {
  statement {
    sid       = "Bucket"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lake.arn]
  }
  statement {
    sid       = "Objects"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.lake.arn}/*"]
  }
  statement {
    sid = "Catalog"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables",
      "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable", "glue:GetPartitions",
      "glue:BatchGetPartition", "glue:CreateTableOptimizer", "glue:GetTableOptimizer",
      "glue:BatchGetTableOptimizer", "glue:CreateDatabase",
    ]
    resources = concat(
      ["arn:${data.aws_partition.this.partition}:glue:${var.region}:${local.account}:catalog"],
      [for d in local.databases : "arn:${data.aws_partition.this.partition}:glue:${var.region}:${local.account}:database/${d}"],
      [for d in local.databases : "arn:${data.aws_partition.this.partition}:glue:${var.region}:${local.account}:table/${d}/*"],
    )
  }
  statement {
    sid = "Query"
    actions = [
      "athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults",
      "athena:StopQueryExecution", "athena:GetWorkGroup",
    ]
    resources = [aws_athena_workgroup.ct.arn]
  }
  statement {
    sid       = "PassOptimizerRole"
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.optimizer.arn]
  }
}

resource "aws_iam_role_policy" "demo" {
  name   = "lakehouse"
  role   = aws_iam_role.demo.id
  policy = data.aws_iam_policy_document.demo.json
}

# ---- Glue table optimizers (compaction, snapshot retention, orphan files; ADR 0012 on AWS) ----
data "aws_iam_policy_document" "assume_from_glue" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "optimizer" {
  name               = "control-tower-glue-optimizer"
  assume_role_policy = data.aws_iam_policy_document.assume_from_glue.json
}

data "aws_iam_policy_document" "optimizer" {
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
  }
  statement {
    actions = ["glue:GetTable", "glue:UpdateTable", "glue:GetDatabase"]
    resources = concat(
      ["arn:${data.aws_partition.this.partition}:glue:${var.region}:${local.account}:catalog"],
      [for d in local.databases : "arn:${data.aws_partition.this.partition}:glue:${var.region}:${local.account}:database/${d}"],
      [for d in local.databases : "arn:${data.aws_partition.this.partition}:glue:${var.region}:${local.account}:table/${d}/*"],
    )
  }
  statement {
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:${data.aws_partition.this.partition}:logs:${var.region}:${local.account}:log-group:/aws-glue/*"]
  }
}

resource "aws_iam_role_policy" "optimizer" {
  name   = "optimizer"
  role   = aws_iam_role.optimizer.id
  policy = data.aws_iam_policy_document.optimizer.json
}

# ---- cost guard ------------------------------------------------------------------------------
resource "aws_budgets_budget" "lakehouse" {
  for_each     = { low = 5, high = 20 }
  name         = "control-tower-lakehouse-${each.key}"
  budget_type  = "COST"
  limit_amount = tostring(each.value)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"
  cost_filter {
    name   = "TagKeyValue"
    values = ["user:project$delivery-control-tower"]
  }
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_email]
  }
}
