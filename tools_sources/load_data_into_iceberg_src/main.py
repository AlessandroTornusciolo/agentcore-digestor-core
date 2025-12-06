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

        env = os.environ.get("ENV", "dev")
        output_bucket = f"agentcore-digestor-tables-{env}"

        # ------------------------------------------------------------------
        # 1. Parse S3 path
        # ------------------------------------------------------------------
        bucket = file_s3_path.replace("s3://", "").split("/")[0]
        key    = "/".join(file_s3_path.replace("s3://", "").split("/")[1:])

        # ------------------------------------------------------------------
        # 2. Read CSV into pandas DataFrame
        # ------------------------------------------------------------------
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj["Body"].read().decode("utf-8")

        rows = list(csv.DictReader(io.StringIO(data)))

        if not rows:
            return {"status": "failed", "error": "No rows to load"}

        df = pd.DataFrame(rows)

        # ------------------------------------------------------------------
        # 3. Write DataFrame as Parquet into Iceberg table folder
        #    (Iceberg will handle metadata updates)
        # ------------------------------------------------------------------
        write_path = f"s3://{output_bucket}/{table_name}/data/"

        wr.s3.to_parquet(
            df=df,
            path=write_path,
            dataset=True,       # write as partitioned dataset
            mode="append"       # append new files
        )

        return {
            "status": "success",
            "records_loaded": len(df)
        }

    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
            "stack_trace": repr(e)
        }
