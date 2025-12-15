import json
import pandas as pd
import boto3
import io
from datetime import datetime

s3 = boto3.client("s3")


# -------------------------------------------------------
# Utility: Extract bucket + key from s3:// URL
# -------------------------------------------------------
def parse_s3_path(path: str):
    path = path.replace("s3://", "")
    bucket = path.split("/")[0]
    key = "/".join(path.split("/")[1:])
    return bucket, key


# -------------------------------------------------------
# Utility: Detect delimiter from a sample
# -------------------------------------------------------
def detect_delimiter(sample: str):
    candidates = [",", ";", "\t", "|"]
    counts = {d: sample.count(d) for d in candidates}
    return max(counts, key=counts.get)


# -------------------------------------------------------
# Utility: Extract domain, dataset, optional
# -------------------------------------------------------
def extract_name_parts(filename: str):
    base = filename.rsplit(".", 1)[0]
    parts = base.split("_")

    if len(parts) < 2:
        return None, None, None

    domain = parts[0]
    dataset = parts[1]
    optional = "_".join(parts[2:]) if len(parts) > 2 else None
    return domain, dataset, optional


# -------------------------------------------------------
# Utility: Summaries
# -------------------------------------------------------
def summarize_tabular(df: pd.DataFrame):
    return f"File tabellare con {len(df.columns)} colonne."


def summarize_json(obj):
    if isinstance(obj, dict):
        return f"JSON con {len(obj.keys())} campi principali."
    if isinstance(obj, list):
        if not obj:
            return "JSON array vuoto."
        if isinstance(obj[0], dict):
            return f"JSON array di oggetti con {len(obj[0].keys())} campi."
    return "Contenuto JSON non strutturato."


# -------------------------------------------------------
# Lambda handler
# -------------------------------------------------------
def handler(event, context):
    try:
        file_s3_path = event["file_s3_path"]
        sheet = event.get("sheet")

        bucket, key = parse_s3_path(file_s3_path)
        filename = key.split("/")[-1]
        extension = filename.lower().split(".")[-1]

        obj = s3.get_object(Bucket=bucket, Key=key)
        raw_bytes = obj["Body"].read()
        text_sample = raw_bytes[:5000].decode("utf-8", errors="ignore")

        domain, dataset, optional = extract_name_parts(filename)

        # -------------------------------------------------------
        # CSV / TSV / TXT
        # -------------------------------------------------------
        if extension in ["csv", "tsv", "txt"]:
            delimiter = "\t" if extension == "tsv" else detect_delimiter(text_sample)

            try:
                df = pd.read_csv(io.BytesIO(raw_bytes), delimiter=delimiter, nrows=50)
            except Exception:
                return {
                    "status": "failed",
                    "file_type": "text_unstructured",
                    "ready_for_ingestion": False,
                    "message": "File non tabellare o delimitatore non valido"
                }

            return {
                "status": "success",
                "file_name": filename,
                "extension": extension,
                "file_type": "csv" if delimiter == "," else "delimited_text",
                "delimiter": delimiter,
                "domain": domain,
                "dataset": dataset,
                "name_optional": optional,
                "structured": True,
                "columns": list(df.columns),
                "content_summary": summarize_tabular(df),
                "ready_for_ingestion": domain is not None and dataset is not None,
            }

        # -------------------------------------------------------
        # NDJSON
        # -------------------------------------------------------
        if extension == "ndjson":
            lines = text_sample.strip().split("\n")
            try:
                first = json.loads(lines[0])
            except Exception:
                return {
                    "status": "failed",
                    "file_type": "jsonl_invalid",
                    "ready_for_ingestion": False
                }

            return {
                "status": "success",
                "file_name": filename,
                "file_type": "jsonl",
                "domain": domain,
                "dataset": dataset,
                "name_optional": optional,
                "structured": True,
                "columns": list(first.keys()),
                "content_summary": summarize_tabular(pd.json_normalize(first)),
                "ready_for_ingestion": domain is not None and dataset is not None,
            }

        # -------------------------------------------------------
        # JSON
        # -------------------------------------------------------
        if extension == "json":
            try:
                parsed = json.loads(text_sample)
            except Exception:
                return {
                    "status": "failed",
                    "file_type": "json_invalid",
                    "ready_for_ingestion": False
                }

            if isinstance(parsed, list):
                if not parsed:
                    return {
                        "status": "warning",
                        "file_type": "json_array_empty",
                        "ready_for_ingestion": False
                    }

                if isinstance(parsed[0], dict):
                    return {
                        "status": "warning",
                        "file_type": "json_array",
                        "domain": domain,
                        "dataset": dataset,
                        "name_optional": optional,
                        "structured": True,
                        "columns": list(parsed[0].keys()),
                        "content_summary": summarize_json(parsed),
                        "ready_for_ingestion": domain is not None and dataset is not None
                    }

                return {
                    "status": "failed",
                    "file_type": "json_unstructured_array",
                    "ready_for_ingestion": False
                }

            if isinstance(parsed, dict):
                return {
                    "status": "success",
                    "file_name": filename,
                    "file_type": "json_object",
                    "domain": domain,
                    "dataset": dataset,
                    "name_optional": optional,
                    "structured": True,
                    "columns": list(parsed.keys()),
                    "content_summary": summarize_json(parsed),
                    "ready_for_ingestion": domain is not None and dataset is not None
                }

        # -------------------------------------------------------
        # Excel
        # -------------------------------------------------------
        if extension in ["xlsx", "xls"]:
            excel = pd.ExcelFile(io.BytesIO(raw_bytes))
            sheet_to_use = sheet or excel.sheet_names[0]
            df = excel.parse(sheet_to_use, nrows=50)

            return {
                "status": "success",
                "file_name": filename,
                "file_type": "excel",
                "domain": domain,
                "dataset": dataset,
                "name_optional": optional,
                "sheet_used": sheet_to_use,
                "structured": True,
                "columns": list(df.columns),
                "content_summary": summarize_tabular(df),
                "ready_for_ingestion": domain is not None and dataset is not None
            }

        return {
            "status": "failed",
            "file_type": "unsupported",
            "ready_for_ingestion": False
        }

    except Exception as e:
        return {
            "status": "failed",
            "error": str(e)
        }
