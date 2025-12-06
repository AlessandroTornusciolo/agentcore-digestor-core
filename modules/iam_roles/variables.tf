variable "env" {
  type        = string
  description = "Environment such as dev, test, prod"
}

variable "roles" {
  description = "Map of IAM roles to create"

  type = map(object({
    role_name      = string
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
