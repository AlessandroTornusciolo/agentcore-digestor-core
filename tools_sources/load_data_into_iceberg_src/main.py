import boto3
import csv
import io
import pandas as pd
import awswrangler as wr
import os

s3 = boto3.client("s3")


def handler(event, context):
    try:
        file_s3_path = event["file_s3_path"]   # MUST be normalized_path
        table_name = event["table_name"]
        schema = event.get("schema")

        if not schema:
            return {
                "status": "failed",
                "error": "Missing 'schema' in event payload"
            }

        env = os.environ.get("ENV", "dev")
        warehouse_bucket = f"agentcore-digestor-iceberg-bronze-{env}"
        write_path = f"s3://{warehouse_bucket}/warehouse/{table_name}/data/"

        # ----------------------------------------------------
        # Read NORMALIZED CSV
        # ----------------------------------------------------
        bucket = file_s3_path.replace("s3://", "").split("/")[0]
        key = "/".join(file_s3_path.replace("s3://", "").split("/")[1:])

        obj = s3.get_object(Bucket=bucket, Key=key)
        raw_data = obj["Body"].read().decode("utf-8")

        rows = list(csv.DictReader(io.StringIO(raw_data)))
        if not rows:
            return {"status": "failed", "error": "No rows to load"}

        df = pd.DataFrame(rows)

        # ----------------------------------------------------
        # Apply schema casting (defensive)
        # ----------------------------------------------------
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
            else:
                df[col] = df[col].astype(str)

        # ----------------------------------------------------
        # Write Parquet
        # ----------------------------------------------------
        wr.s3.to_parquet(
            df=df,
            path=write_path,
            dataset=True,
            mode="append"
        )

        return {
            "status": "success",
            "records_loaded": len(df),
            "warehouse_path": write_path
        }

    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
            "stack_trace": repr(e)
        }
