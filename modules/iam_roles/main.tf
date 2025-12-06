resource "aws_iam_role" "iam_role" {
  for_each = var.roles

  name = each.value.role_name

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      for svc in each.value.assume_services :
      {
        Effect = "Allow"
        Principal = { Service = svc }
        Action   = "sts:AssumeRole"
      }
    ]
  })

  tags = merge({
    Project     = "agentcore"
    Module      = "digestor"
    Environment = var.env
  }, each.value.tags)
}

locals {
  iam_role_policies = merge([
    for role_key, role_value in var.roles : {
      for policy_key, policy_value in role_value.inline_policies :
      "${role_key}.${policy_key}" => {
        role_key    = role_key
        role_name   = role_value.role_name
        policy_name = policy_value.policy_name
        statements  = policy_value.statements
      }
    }
  ]...)
}

resource "aws_iam_role_policy" "iam_role_policy" {
  for_each = local.iam_role_policies

  name = each.value.policy_name
  role = aws_iam_role.iam_role[each.value.role_key].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      for stmt in each.value.statements : {
        Effect   = stmt.effect
        Action   = stmt.actions
        Resource = stmt.resources
      }
    ]
  })
}
