import boto3
import json
import os
import time
import uuid

athena = boto3.client("athena")
glue = boto3.client("glue")

def wait_for_athena(query_id):
    while True:
        resp = athena.get_query_execution(QueryExecutionId=query_id)
        state = resp["QueryExecution"]["Status"]["State"]

        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return resp

        time.sleep(1)

def handler(event, context):
    try:
        env = os.environ.get("ENV", "dev")

        table_name = event["table_name"]      # final Iceberg table
        schema     = event["schema"]          # list of {"name":..., "type":...}

        db_name = f"agentcore_digestor_db_{env}"

        # S3 dove sono già i Parquet scritti dalla lambda load_into_iceberg
        bucket = f"agentcore-digestor-tables-{env}"
        prefix = f"{table_name}/data/"

        # Nome tabella di staging
        staging = f"{table_name}_staging_{uuid.uuid4().hex[:6]}"

        # 1) STAGING TABLE esterna Parquet minima in Glue
        glue_columns = [
            {"Name": col["name"], "Type": col["type"]}
            for col in schema
        ]

        glue.create_table(
            DatabaseName=db_name,
            TableInput={
                "Name": staging,
                "TableType": "EXTERNAL_TABLE",
                "StorageDescriptor": {
                    "Columns": glue_columns,
                    "Location": f"s3://{bucket}/{prefix}",
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                    }
                },
                # NESSUN parametro “EXTERNAL=TRUE”, NESSUN “classification”
                "Parameters": {}
            }
        )

        # 2) CTAS per creare la Iceberg MANAGED (qui aggiungiamo is_external=false)
        warehouse = f"s3://agentcore-digestor-athena-results-{env}/results/db/{table_name}/"

        query = f"""
        CREATE TABLE {db_name}.{table_name}
        WITH (
            table_type='ICEBERG',
            format='PARQUET',
            is_external=false,
            location='{warehouse}'
        ) AS
        SELECT * FROM {db_name}.{staging};
        """

        output_bucket = f"s3://agentcore-digestor-athena-results-{env}/results/"

        q = athena.start_query_execution(
            QueryString=query,
            WorkGroup="primary",
            ResultConfiguration={"OutputLocation": output_bucket}
        )

        result = wait_for_athena(q["QueryExecutionId"])
        state = result["QueryExecution"]["Status"]["State"]

        # 3) Drop della staging table comunque
        glue.delete_table(DatabaseName=db_name, Name=staging)

        if state != "SUCCEEDED":
            return {
                "status": "failed",
                "error": "CTAS operation failed",
                "athena_state": state,
                "query": query
            }

        return {
            "status": "success",
            "message": "Managed Iceberg table successfully created via CTAS",
            "table_name": table_name
        }

    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
            "stack_trace": repr(e)
        }
