import json
import boto3
import csv
import io
import pandas as pd
import awswrangler as wr
import os

s3 = boto3.client("s3")


def handler(event, context):
    try:
        file_s3_path = event["file_s3_path"]
        table_name   = event["table_name"]
        schema       = event.get("schema")  # MUST BE PROVIDED BY AGENT

        if schema is None:
            return {
                "status": "failed",
                "error": "Missing 'schema' in event payload"
            }

        env = os.environ.get("ENV", "dev")

        # --------------------------------------------------------------------
        # NEW: Iceberg Warehouse Bucket (Bronze)
        # --------------------------------------------------------------------
        warehouse_bucket = f"agentcore-digestor-iceberg-bronze-{env}"
        write_path = f"s3://{warehouse_bucket}/warehouse/{table_name}/data/"

        # --------------------------------------------------------------------
        # 1. Parse S3 path of the ORIGINAL file
        # --------------------------------------------------------------------
        bucket = file_s3_path.replace("s3://", "").split("/")[0]
        key    = "/".join(file_s3_path.replace("s3://", "").split("/")[1:])

        # --------------------------------------------------------------------
        # 2. Read CSV into DataFrame
        # --------------------------------------------------------------------
        obj = s3.get_object(Bucket=bucket, Key=key)
        raw_data = obj["Body"].read().decode("utf-8")

        rows = list(csv.DictReader(io.StringIO(raw_data)))

        if not rows:
            return {"status": "failed", "error": "No rows to load"}

        df = pd.DataFrame(rows)

        # --------------------------------------------------------------------
        # 3. Apply AGENT-NORMALIZED schema conversions
        # --------------------------------------------------------------------
        for colinfo in schema:
            col = colinfo["name"]
            coltype = colinfo["type"]

            if col not in df.columns:
                continue

            if coltype == "int":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

            elif coltype == "float":
                df[col] = pd.to_numeric(df[col], errors="coerce")

            elif coltype == "datetime":
                df[col] = pd.to_datetime(df[col], errors="coerce")

            elif coltype == "string":
                df[col] = df[col].astype(str)

            else:
                df[col] = df[col].astype(str)

        # --------------------------------------------------------------------
        # 4. Write DataFrame as PARQUET into Iceberg Warehouse
        # --------------------------------------------------------------------
        wr.s3.to_parquet(
            df=df,
            path=write_path,
            dataset=True,
            mode="append"
        )

        return {
            "status": "success",
            "records_loaded": len(df),
            "warehouse_path": write_path,
            "message": "Dataframe converted and written successfully."
        }

    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
            "stack_trace": repr(e)
        }
