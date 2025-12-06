output "api_endpoint" {
  value = "${aws_apigatewayv2_api.ingestion_api.api_endpoint}/${aws_apigatewayv2_stage.dev.name}"
}
