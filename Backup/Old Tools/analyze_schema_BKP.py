import json
import boto3
import pandas as pd
from io import BytesIO
from strands import tool

s3 = boto3.client("s3")


@tool
def analyze_schema(file_s3_path: str, file_format: str, max_rows: int = 50) -> dict:
    """
    Infers schema for tabular / semi-tabular files.

    REQUIRED:
      - file_s3_path: S3 URI of the (possibly converted) file
      - file_format: one of ["csv", "tsv", "txt", "ndjson", "json_array"]

    The Agent MUST obtain file_format from:
      - detect_file_type / convert_semi_tabular → converted_format
    """

    # -------------------------------
    # Parse S3 path
    # -------------------------------
    bucket = file_s3_path.replace("s3://", "").split("/")[0]
    key = "/".join(file_s3_path.replace("s3://", "").split("/")[1:])

    obj = s3.get_object(Bucket=bucket, Key=key)
    raw_bytes = obj["Body"].read()

    # Normalize format string
    fmt = (file_format or "").lower()

    # ==========================================================================
    # CASE 1 — NDJSON (JSON Lines)
    # ==========================================================================
    if fmt in ("ndjson", "jsonl"):
        lines = raw_bytes.decode("utf-8").splitlines()
        records = []

        for line in lines[:max_rows]:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except Exception:
                # Skip malformed JSON lines
                continue

        if not records:
            return {
                "status": "failed",
                "error": "NDJSON file contains no valid JSON lines."
            }

        df = pd.DataFrame(records)

    # ==========================================================================
    # CASE 2 — JSON ARRAY (list of objects)
    # ==========================================================================
    elif fmt == "json_array":
        try:
            data = json.loads(raw_bytes.decode("utf-8"))
        except Exception as e:
            return {
                "status": "failed",
                "error": f"Invalid JSON content: {str(e)}"
            }

        if not isinstance(data, list):
            return {
                "status": "failed",
                "error": "JSON is not an array of objects."
            }

        if not data:
            return {
                "status": "failed",
                "error": "JSON array is empty."
            }

        df = pd.DataFrame(data[:max_rows])

    # ==========================================================================
    # CASE 3 — CSV / TSV / TXT (delimited text)
    # ==========================================================================
    elif fmt in ("csv", "tsv", "txt", ""):
        # Let pandas auto-detect the delimiter, except for TSV
        if fmt == "tsv":
            df = pd.read_csv(BytesIO(raw_bytes), nrows=max_rows, sep="\t")
        else:
            # sep=None + engine="python" lets pandas sniff the delimiter
            df = pd.read_csv(BytesIO(raw_bytes), nrows=max_rows, sep=None, engine="python")

    # ==========================================================================
    # Unsupported format for this tool
    # ==========================================================================
    else:
        return {
            "status": "failed",
            "error": f"Unsupported file_format for analyze_schema: {file_format}"
        }

    # ==========================================================================
    # Infer schema
    # ==========================================================================
    schema = []
    for col in df.columns:
        dtype = df[col].dtype

        if pd.api.types.is_integer_dtype(dtype):
            t = "int"
        elif pd.api.types.is_float_dtype(dtype):
            t = "float"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            t = "datetime"
        else:
            t = "string"

        schema.append({"name": col, "type": t})

    return {
        "status": "success",
        "rows_analyzed": len(df),
        "schema": schema
    }
