resource "aws_glue_catalog_database" "ingestion_db" {
  name = "agentcore_digestor_db_${var.env}"

  location_uri = "s3://agentcore-digestor-athena-results-${var.env}/results/db/agentcore_digestor_db_${var.env}/"

  parameters = {
    "hive.metastore.glue.default-warehouse-dir" = "s3://agentcore-digestor-athena-results-${var.env}/results/db/"
  }

  tags = {
    Project     = "agentcore"
    Module      = "digestor"
    Environment = var.env
  }
}
