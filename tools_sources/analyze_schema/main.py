import boto3
import pandas as pd
import json
import io

s3 = boto3.client("s3")

SUPPORTED_FORMATS = {"csv", "tsv", "txt", "ndjson"}


def infer_dtype(series: pd.Series):
    if pd.api.types.is_integer_dtype(series):
        return "int"
    if pd.api.types.is_float_dtype(series):
        return "float"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "datetime"
    return "string"


def handler(event, context):
    try:
        file_s3_path = event["file_s3_path"]
        file_format = event["file_format"]
        max_rows = event.get("max_rows", 50)

        if file_format not in SUPPORTED_FORMATS:
            return {
                "status": "failed",
                "error": f"Unsupported format for schema analysis: {file_format}"
            }

        # --------------------------------------------------
        # Load file from S3
        # --------------------------------------------------
        bucket = file_s3_path.replace("s3://", "").split("/")[0]
        key = "/".join(file_s3_path.replace("s3://", "").split("/")[1:])

        obj = s3.get_object(Bucket=bucket, Key=key)
        raw_bytes = obj["Body"].read()

        # --------------------------------------------------
        # Parse into DataFrame
        # --------------------------------------------------
        if file_format == "ndjson":
            lines = raw_bytes.decode("utf-8").splitlines()
            records = [json.loads(l) for l in lines[:max_rows]]
            df = pd.DataFrame(records)

        else:
            df = pd.read_csv(
                io.BytesIO(raw_bytes),
                nrows=max_rows
            )

        # --------------------------------------------------
        # Infer schema
        # --------------------------------------------------
        schema = []
        for col in df.columns:
            dtype = infer_dtype(df[col])
            schema.append({
                "name": col,
                "type": dtype
            })

        return {
            "status": "success",
            "rows_analyzed": len(df),
            "columns": len(schema),
            "schema": schema
        }

    except Exception as e:
        return {
            "status": "failed",
            "error": str(e)
        }
