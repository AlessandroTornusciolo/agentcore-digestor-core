variable "env" {
  type        = string
  description = "Environment (dev, test, prod)"
}

variable "buckets" {
  description = "Map of S3 buckets to create"

  type = map(object({
    name              = string
    tags              = map(string)

    enable_versioning = bool
    versioning_status = string     # Enabled / Suspended

    enable_encryption = bool
    encryption_type   = string     # AES256 / aws:kms

    policy             = optional(string)
  }))
}
