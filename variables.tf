variable "aws_profile" {
  type        = string
  description = "AWS profile"
  default     = "default"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "env" {
  description = "Execution environment"
  type        = string
}

variable "s3_buckets" {
  description = "Map of S3 buckets to create"
  type = map(object({
    name              = string
    tags              = map(string)

    enable_versioning = bool
    versioning_status = string

    enable_encryption = bool
    encryption_type   = string

    policy             = optional(string)
  }))
}

variable "iam_roles" {
  description = "IAM roles configuration"
  type = map(object({
    role_name       = string
    assume_services = list(string)

    inline_policies = map(object({
      policy_name = string
      statements = list(object({
        effect    = string
        actions   = list(string)
        resources = list(string)
      }))
    }))

    tags = map(string)
  }))
}

variable "ecr_repositories" {
  description = "ECR repositories configuration"
  type = map(object({
    component    = string
    scan_on_push = bool
    tags         = map(string)
  }))
}

variable "layers" {
  description = "Map of Lambda layer definitions"
  type = map(object({
    layer_name  = string
    filename    = optional(string)
    runtimes    = list(string)
    tags        = map(string)
    description = optional(string)
    arn         = optional(string)   # only for public/external layers
  }))
}

variable "lambdas" {
  description = "Definitions for all lambda functions"
  type = map(object({
    function_name = string
    timeout       = number
    env_vars      = map(string)
    tags          = map(string)
    package_type  = string                # Zip or Image
    image_uri     = optional(string)
    runtime       = optional(string)
    handler       = optional(string)
    source_path   = optional(string)
    layer_names   = optional(list(string), [])
  }))
}

