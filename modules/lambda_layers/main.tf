resource "aws_lambda_layer_version" "layer" {
  for_each = {
    for k, v in var.layers :
    k => v
    if !contains(keys(v), "arn")
  }

  layer_name          = each.value.layer_name
  filename            = each.value.filename
  source_code_hash    = filebase64sha256(each.value.filename)
  compatible_runtimes = each.value.runtimes
  description         = lookup(each.value, "description", null)
}
