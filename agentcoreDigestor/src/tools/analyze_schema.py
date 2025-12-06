from strands import tool
import pandas as pd
import boto3
import io

s3 = boto3.client("s3")


@tool
def analyze_schema(file_s3_path: str) -> dict:
    """
    Reads a CSV file from S3 and infers schema (very simple version).
    """

    bucket, key = file_s3_path.replace("s3://", "").split("/", 1)

    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()

    # CSV → Pandas
    df = pd.read_csv(io.BytesIO(body))

    schema = []
    for col, dtype in df.dtypes.items():
        schema.append({"name": col, "type": str(dtype)})

    return {
        "status": "success",
        "schema": schema,
        "sample_rows": len(df)
    }
