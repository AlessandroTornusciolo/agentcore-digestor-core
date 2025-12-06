import json
import boto3
import csv
import io

s3 = boto3.client("s3")

# ------------------------------------------------------------
# Infer simple type (string | int | float | bool)
# ------------------------------------------------------------
def infer_type(value):
    if value is None or value == "":
        return "string"
    try:
        int(value)
        return "int"
    except:
        pass
    try:
        float(value)
        return "float"
    except:
        pass
    if value.lower() in ("true", "false"):
        return "bool"
    return "string"


def handler(event, context):
    try:
        file_s3_path = event["file_s3_path"]

        bucket = file_s3_path.replace("s3://", "").split("/")[0]
        key    = "/".join(file_s3_path.replace("s3://", "").split("/")[1:])

        # Read file
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj["Body"].read().decode("utf-8")

        rows = list(csv.DictReader(io.StringIO(data)))

        if not rows:
            return {"status": "failed", "error": "Empty file"}

        header = rows[0].keys()

        schema = []
        for h in header:
            sample_value = rows[0][h]
            schema.append({
                "name": h,
                "type": infer_type(sample_value)
            })

        return {
            "status": "success",
            "schema": schema,
            "sample_rows": min(5, len(rows))
        }

    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
            "stack_trace": repr(e)
        }
