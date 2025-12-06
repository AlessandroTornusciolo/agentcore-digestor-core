variable "env" {
  type = string
}

variable "layers" {
  description = "Map of layer definitions"
  type = map(object({
    layer_name  = string
    filename    = optional(string)
    runtimes    = list(string)
    tags        = map(string)
    description = optional(string)
    arn         = optional(string) 
  }))
}
