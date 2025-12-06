output "role_arns" {
  description = "IAM role ARNs"
  value       = { for k, r in aws_iam_role.iam_role : k => r.arn }
}
