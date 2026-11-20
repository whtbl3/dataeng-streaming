output "bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "demo_role_arn" {
  description = "Assume this role for the demo (the jobs use only these permissions)"
  value       = aws_iam_role.demo.arn
}

output "optimizer_role_arn" {
  value = aws_iam_role.optimizer.arn
}

output "athena_workgroup" {
  value = aws_athena_workgroup.ct.name
}
