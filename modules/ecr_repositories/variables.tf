variable "env" {
  type        = string
  description = "Environment (dev, test, prod)"
}

variable "repositories" {
  description = "Map of ECR repositories to create"
  type = map(object({
    component    = string      
    scan_on_push = bool
    tags         = map(string)
  }))
}
