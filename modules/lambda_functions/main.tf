resource "aws_lambda_function" "lambda" {
  for_each = var.lambdas

  function_name = each.value.function_name
  role          = each.value.role_arn
  timeout       = each.value.timeout
  package_type  = each.value.package_type

  tags = merge(
    {
      Project     = "agentcore"
      Module      = "digestor"
      Environment = var.env
    },
    each.value.tags
  )

  #################################
  # ZIP-BASED LAMBDA
  #################################
  runtime          = each.value.package_type == "Zip" ? each.value.runtime : null
  handler          = each.value.package_type == "Zip" ? each.value.handler : null
  filename         = each.value.package_type == "Zip" ? each.value.source_path : null
  source_code_hash = each.value.package_type == "Zip" ? filebase64sha256(each.value.source_path) : null
  layers           = each.value.package_type == "Zip" ? each.value.layers : []

  #################################
  # IMAGE-BASED LAMBDA
  #################################
  image_uri = each.value.package_type == "Image" ? each.value.image_uri : null

  environment {
    variables = each.value.env_vars
  }
}
