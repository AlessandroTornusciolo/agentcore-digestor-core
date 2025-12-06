resource "aws_apigatewayv2_api" "ingestion_api" {
  name          = "agentcore-digestor-api-${var.env}"
  protocol_type = "HTTP"

  tags = {
    Project     = "agentcore"
    Module      = "digestor"
    Environment = var.env
  }
}

resource "aws_apigatewayv2_integration" "lambda_core" {
  api_id           = aws_apigatewayv2_api.ingestion_api.id
  integration_type = "AWS_PROXY"
  integration_uri  = var.lambda_core_invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "ingest_post" {
  api_id    = aws_apigatewayv2_api.ingestion_api.id
  route_key = "POST /ingest"
  target    = "integrations/${aws_apigatewayv2_integration.lambda_core.id}"
}

resource "aws_lambda_permission" "api_invoke_lambda_core" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = var.lambda_core_arn
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.ingestion_api.execution_arn}/*/*"
}

resource "aws_apigatewayv2_stage" "dev" {
  api_id      = aws_apigatewayv2_api.ingestion_api.id
  name        = var.env
  auto_deploy = true
}
