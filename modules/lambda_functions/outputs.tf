output "lambda_arns" {
  description = "ARNs of all lambdas"
  value = { for k, v in aws_lambda_function.lambda : k => v.arn }
}
