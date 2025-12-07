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
        schema       = event.get("schema")  # ← NEW

        if schema is None:
            return {
                "status": "failed",
                "error": "Missing 'schema' in event payload"
            }

        env = os.environ.get("ENV", "dev")
        output_bucket = f"agentcore-digestor-tables-{env}"

        # --------------------------------------------------------------
        # 1. Parse S3 path
        # --------------------------------------------------------------
        bucket = file_s3_path.replace("s3://", "").split("/")[0]
        key    = "/".join(file_s3_path.replace("s3://", "").split("/")[1:])

        # --------------------------------------------------------------
        # 2. Read CSV into DataFrame
        # --------------------------------------------------------------
        obj = s3.get_object(Bucket=bucket, Key=key)
        raw_data = obj["Body"].read().decode("utf-8")

        rows = list(csv.DictReader(io.StringIO(raw_data)))

        if not rows:
            return {"status": "failed", "error": "No rows to load"}

        df = pd.DataFrame(rows)

        # --------------------------------------------------------------
        # 3. Apply type conversions based on agent-normalized schema
        # --------------------------------------------------------------
        for colinfo in schema:
            col = colinfo["name"]
            coltype = colinfo["type"]

            if col not in df.columns:
                continue  # skip missing columns

            # Convert based on type
            if coltype == "int":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

            elif coltype == "float":
                df[col] = pd.to_numeric(df[col], errors="coerce")

            elif coltype == "datetime":
                df[col] = pd.to_datetime(df[col], errors="coerce")

            elif coltype == "string":
                df[col] = df[col].astype(str)

            # Unknown type → cast to string
            else:
                df[col] = df[col].astype(str)

        # --------------------------------------------------------------
        # 4. Write fully converted DataFrame to Parquet
        # --------------------------------------------------------------
        write_path = f"s3://{output_bucket}/{table_name}/data/"

        wr.s3.to_parquet(
            df=df,
            path=write_path,
            dataset=True,
            mode="append"
        )

        return {
            "status": "success",
            "records_loaded": len(df),
            "message": "Dataframe converted and written successfully."
        }

    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
            "stack_trace": repr(e)
        }
