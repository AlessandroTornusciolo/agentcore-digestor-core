import json
import boto3
import os

glue = boto3.client("glue")


def handler(event, context):
    try:
        table_name = event["table_name"]
        schema     = event["schema"]
        partition_keys = event.get("partition_keys", [])

        env = os.environ.get("ENV", "dev")
        db_name = f"agentcore_digestor_db_{env}"

        # Iceberg table path
        table_location = f"s3://agentcore-digestor-tables-{env}/{table_name}/"

        # ----------------------------------------------------------------------
        # 1. Check if table already exists
        # ----------------------------------------------------------------------
        try:
            glue.get_table(DatabaseName=db_name, Name=table_name)
            return {
                "status": "exists",
                "table_location": table_location
            }
        except glue.exceptions.EntityNotFoundException:
            pass

        # ----------------------------------------------------------------------
        # 2. Transform schema → Glue format
        # ----------------------------------------------------------------------
        glue_columns = []
        for col in schema:
            # For MVP, all columns are string-based
            glue_columns.append({
                "Name": col["name"],
                "Type": "string"
            })

        glue_partition_keys = [
            {"Name": p, "Type": "string"} for p in partition_keys
        ]

        # ----------------------------------------------------------------------
        # 3. Create Iceberg table
        # ----------------------------------------------------------------------
        glue.create_table(
            DatabaseName=db_name,
            TableInput={
                "Name": table_name,
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {
                    "table_type": "ICEBERG"
                },
                "StorageDescriptor": {
                    "Columns": glue_columns,
                    "Location": table_location
                },
                "PartitionKeys": glue_partition_keys
            }
        )

        return {
            "status": "created",
            "table_location": table_location
        }

    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
            "stack_trace": repr(e)
        }
