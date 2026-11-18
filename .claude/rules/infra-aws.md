---
paths: ["infra/aws/**"]
---
# Terraform / AWS rules
- Every resource gets the default tag `project=delivery-control-tower` (provider default_tags).
- No NAT Gateway, and nothing that survives `terraform destroy` (S3 buckets use force_destroy). See ADR 0005.
- No credentials in code or tfvars committed; use variables with `.tfvars.example` and IAM roles.
- Least-privilege IAM: scope policies to this project's ARNs, never `"Resource": "*"` with write actions.
- Any change that adds hourly cost must update the cost estimate in ADR 0005.
