variable "env" {
  type = string
}

variable "lambda_core_arn" {
  description = "ARN of the lambda core function"
  type        = string
}

variable "lambda_core_invoke_arn" {
  description = "Invoke ARN of the lambda core function"
  type        = string
}
