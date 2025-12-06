variable "env" {
  type = string
}

variable "lambdas" {
  description = "Map of lambda function configurations"

  type = map(object({
    function_name = string
    timeout       = number
    env_vars      = map(string)
    tags          = map(string)
    role_arn      = string
    package_type  = string                  # "Zip" or "Image"

    # For Image Lambdas
    image_uri     = optional(string)

    # For Zip Lambdas
    runtime       = optional(string)
    handler       = optional(string)
    source_path   = optional(string)
    layers        = optional(list(string), [])
  }))
}
