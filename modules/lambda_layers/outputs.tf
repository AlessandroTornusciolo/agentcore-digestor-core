output "layer_arns" {
  value = {
    for k, v in var.layers :
    k => (
      contains(keys(v), "arn")
        ? v.arn
        : aws_lambda_layer_version.layer[k].arn
    )
  }
}
