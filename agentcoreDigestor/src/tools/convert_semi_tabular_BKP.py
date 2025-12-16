import boto3
import json
import pandas as pd
import io
import csv
from strands import tool
import openpyxl

s3 = boto3.client("s3")

CONVERTED_BUCKET = "agentcore-digestor-upload-raw-dev"  # conversion output lives here


def _read_s3_bytes(path: str) -> bytes:
    bucket = path.replace("s3://", "").split("/")[0]
    key = "/".join(path.replace("s3://", "").split("/")[1:])
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read()


def _write_s3_bytes(bucket: str, key: str, content: bytes):
    s3.put_object(Bucket=bucket, Key=key, Body=content)


@tool
def convert_semi_tabular(file_s3_path: str,
                         file_type: str,
                         sheet: int = 0) -> dict:
    """
    Converts semi-tabular formats into ingestion-ready tabular format.

    Supported conversions:
      - JSON ARRAY → NDJSON
      - XLSX/XLS → CSV (sheet 0 or user-specified)
      - TXT → CSV (delimiter autodetection)
      - CSV/TSV/NDJSON → returned unchanged

    Returns:
        {
          "status": "success",
          "converted_path": "<s3://...>",
          "format": "ndjson|csv",
          "message": "..."
        }
    """

    # ------------------------------------------------------------------
    # CSV / TSV / NDJSON → no conversion required
    # ------------------------------------------------------------------
    if file_type in ("csv", "tsv", "ndjson"):
        return {
            "status": "success",
            "converted_path": file_s3_path,
            "format": file_type,
            "message": f"No conversion needed for {file_type}"
        }

    # ------------------------------------------------------------------
    # JSON ARRAY → NDJSON
    # ------------------------------------------------------------------
    if file_type == "json_array":
        raw_bytes = _read_s3_bytes(file_s3_path)
        try:
            arr = json.loads(raw_bytes.decode("utf-8"))
        except Exception as e:
            return {"status": "failed", "error": f"Invalid JSON array: {e}"}

        if not isinstance(arr, list):
            return {"status": "failed", "error": "JSON content is not an array"}

        # Convert array → NDJSON
        lines = []
        for obj in arr:
            if isinstance(obj, dict):
                lines.append(json.dumps(obj))
        ndjson_bytes = ("\n".join(lines)).encode("utf-8")

        # Write NDJSON next to original file
        filename = file_s3_path.split("/")[-1].rsplit(".", 1)[0] + ".ndjson"
        key = f"converted/{filename}"

        _write_s3_bytes(CONVERTED_BUCKET, key, ndjson_bytes)

        return {
            "status": "success",
            "converted_path": f"s3://{CONVERTED_BUCKET}/{key}",
            "format": "ndjson",
            "message": "JSON array converted to NDJSON"
        }

    # ------------------------------------------------------------------
    # XLSX / XLS → CSV
    # ------------------------------------------------------------------
    if file_type == "excel":
        raw_bytes = _read_s3_bytes(file_s3_path)

        try:
            df = pd.read_excel(io.BytesIO(raw_bytes), sheet_name=sheet)
        except Exception as e:
            return {"status": "failed", "error": f"Excel read error: {e}"}

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_bytes = csv_buffer.getvalue().encode("utf-8")

        filename = file_s3_path.split("/")[-1].rsplit(".", 1)[0] + ".csv"
        key = f"converted/{filename}"

        _write_s3_bytes(CONVERTED_BUCKET, key, csv_bytes)

        return {
            "status": "success",
            "converted_path": f"s3://{CONVERTED_BUCKET}/{key}",
            "format": "csv",
            "message": "Excel sheet converted to CSV"
        }

    # ------------------------------------------------------------------
    # TXT → CSV using delimiter autodetection
    # ------------------------------------------------------------------
    if file_type == "txt":
        raw_str = _read_s3_bytes(file_s3_path).decode("utf-8")

        sniffer = csv.Sniffer()
        try:
            dialect = sniffer.sniff(raw_str.splitlines()[0])
            delimiter = dialect.delimiter
        except Exception:
            # fallback
            delimiter = ","

        df = pd.read_csv(io.StringIO(raw_str), delimiter=delimiter)

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_bytes = csv_buffer.getvalue().encode("utf-8")

        filename = file_s3_path.split("/")[-1].rsplit(".", 1)[0] + ".csv"
        key = f"converted/{filename}"

        _write_s3_bytes(CONVERTED_BUCKET, key, csv_bytes)

        return {
            "status": "success",
            "converted_path": f"s3://{CONVERTED_BUCKET}/{key}",
            "format": "csv",
            "message": f"TXT (delimiter='{delimiter}') converted to CSV"
        }

    # ------------------------------------------------------------------
    # Unsupported format
    # ------------------------------------------------------------------
    return {
        "status": "failed",
        "error": f"Unsupported format for conversion: {file_type}"
    }

