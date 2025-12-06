resource "aws_ecr_repository" "repo" {
  for_each = var.repositories

  # Naming convention:
  # agentcore-digestor-ecr-[component]-[env]
  name = "agentcore-digestor-ecr-${each.value.component}-${var.env}"

  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = each.value.scan_on_push
  }

  tags = merge(
    {
      Project     = "agentcore"
      Module      = "digestor"
      Environment = var.env
    },
    each.value.tags
  )
}
