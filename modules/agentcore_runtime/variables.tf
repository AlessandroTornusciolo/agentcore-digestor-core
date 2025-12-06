variable "env" {
  type        = string
  description = "Environment (dev/test/prod)"
}

variable "runtimes" {
  description = "Map of AgentCore runtimes to create"

  type = map(object({
    runtime_name      = string
    foundation_model  = string
    reasoning_config = optional(object({
      max_tokens  = number
      temperature = number
    }), null)

    tags = optional(map(string), {})
  }))
}
