import boto3
import json
import pandas as pd
import io
import csv

s3 = boto3.client("s3")

CONVERTED_BUCKET = "agentcore-digestor-upload-raw-dev"
CONVERTED_PREFIX = "converted"


def parse_s3_path(path: str):
    path = path.replace("s3://", "")
    bucket = path.split("/")[0]
    key = "/".join(path.split("/")[1:])
    return bucket, key


def handler(event, context):
    try:
        file_s3_path = event["file_s3_path"]
        file_type = event["file_type"]
        sheet = event.get("sheet", 0)

        src_bucket, src_key = parse_s3_path(file_s3_path)
        raw_bytes = s3.get_object(Bucket=src_bucket, Key=src_key)["Body"].read()

        filename = src_key.split("/")[-1]
        base = filename.rsplit(".", 1)[0]

        # --------------------------------------------------
        # CSV / TSV / NDJSON → passthrough
        # --------------------------------------------------
        if file_type in ("csv", "tsv", "ndjson"):
            return {
                "status": "success",
                "converted_path": file_s3_path,
                "converted_format": file_type,
                "message": "No conversion required"
            }

        # --------------------------------------------------
        # JSON ARRAY → NDJSON
        # --------------------------------------------------
        if file_type == "json_array":
            arr = json.loads(raw_bytes.decode("utf-8"))

            if not isinstance(arr, list):
                raise ValueError("JSON is not an array")

            lines = [json.dumps(obj) for obj in arr if isinstance(obj, dict)]
            ndjson_bytes = "\n".join(lines).encode("utf-8")

            out_key = f"{CONVERTED_PREFIX}/{base}.ndjson"
            s3.put_object(Bucket=CONVERTED_BUCKET, Key=out_key, Body=ndjson_bytes)

            return {
                "status": "success",
                "converted_path": f"s3://{CONVERTED_BUCKET}/{out_key}",
                "converted_format": "ndjson",
                "message": "JSON array converted to NDJSON"
            }

        # --------------------------------------------------
        # EXCEL → CSV
        # --------------------------------------------------
        if file_type == "excel":
            df = pd.read_excel(io.BytesIO(raw_bytes), sheet_name=sheet)

            buf = io.StringIO()
            df.to_csv(buf, index=False)
            csv_bytes = buf.getvalue().encode("utf-8")

            out_key = f"{CONVERTED_PREFIX}/{base}.csv"
            s3.put_object(Bucket=CONVERTED_BUCKET, Key=out_key, Body=csv_bytes)

            return {
                "status": "success",
                "converted_path": f"s3://{CONVERTED_BUCKET}/{out_key}",
                "converted_format": "csv",
                "message": "Excel converted to CSV"
            }

        # --------------------------------------------------
        # TXT → CSV (delimiter autodetect)
        # --------------------------------------------------
        if file_type == "txt":
            text = raw_bytes.decode("utf-8")

            try:
                dialect = csv.Sniffer().sniff(text.splitlines()[0])
                delimiter = dialect.delimiter
            except Exception:
                delimiter = ","

            df = pd.read_csv(io.StringIO(text), delimiter=delimiter)

            buf = io.StringIO()
            df.to_csv(buf, index=False)
            csv_bytes = buf.getvalue().encode("utf-8")

            out_key = f"{CONVERTED_PREFIX}/{base}.csv"
            s3.put_object(Bucket=CONVERTED_BUCKET, Key=out_key, Body=csv_bytes)

            return {
                "status": "success",
                "converted_path": f"s3://{CONVERTED_BUCKET}/{out_key}",
                "converted_format": "csv",
                "message": f"TXT converted to CSV (delimiter={delimiter})"
            }

        return {
            "status": "failed",
            "error": f"Unsupported file_type: {file_type}"
        }

    except Exception as e:
        return {
            "status": "failed",
            "error": str(e)
        }
