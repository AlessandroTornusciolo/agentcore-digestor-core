resource "aws_agentcore_runtime" "runtime" {
  for_each = var.runtimes

  name              = each.value.runtime_name
  foundation_model  = each.value.foundation_model

  dynamic "reasoning_config" {
    for_each = each.value.reasoning_config != null ? [each.value.reasoning_config] : []
    content {
      max_tokens  = reasoning_config.value.max_tokens
      temperature = reasoning_config.value.temperature
    }
  }

  tags = merge(
    {
      Project     = "agentcore"
      Module      = "digestor"
      Environment = var.env
    },
    lookup(each.value, "tags", {})
  )
}
