import json
import pandas as pd
import boto3
import io
import re
from datetime import datetime
from strands import tool

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
    best = max(counts, key=counts.get)
    return best

# -------------------------------------------------------
# Utility: Extract domain, dataset, optional name
# Pattern: <domain>_<dataset>_<optional>.<ext>
#          <domain>_<dataset>.<ext> also allowed
# -------------------------------------------------------
def extract_name_parts(filename: str):
    base = filename.rsplit(".", 1)[0]  # remove extension
    parts = base.split("_")

    if len(parts) < 2:
        return None, None, None  # not suited for ingestion

    domain = parts[0]
    dataset = parts[1]
    optional = "_".join(parts[2:]) if len(parts) > 2 else None

    return domain, dataset, optional

# -------------------------------------------------------
# Utility: Summarize content
# -------------------------------------------------------
def summarize_tabular(df: pd.DataFrame):
    cols = list(df.columns)
    return f"File tabellare con {len(cols)} colonne."

def summarize_json(json_obj):
    if isinstance(json_obj, dict):
        return f"JSON con {len(json_obj.keys())} campi principali."
    if isinstance(json_obj, list):
        if len(json_obj) == 0:
            return "JSON array vuoto."
        if isinstance(json_obj[0], dict):
            return f"JSON array di oggetti con {len(json_obj[0].keys())} campi."
        return "JSON array di valori non strutturati."
    return "Contenuto JSON non strutturato."

# -------------------------------------------------------
# MAIN TOOL
# -------------------------------------------------------
@tool
def detect_file_type(file_s3_path: str, sheet: str = None):
    """
    Detects the file type, structure, delimiter, and ingestion eligibility.

    Supported:
    - CSV, TSV, TXT
    - JSON (JSONL, JSON array, structured dict)
    - Excel (XLSX, XLS)

    Returns metadata including:
    - domain, dataset, optional name extracted from filename
    - file_type
    - structured/unstructured
    - ready_for_ingestion = true/false
    """

    try:
        bucket, key = parse_s3_path(file_s3_path)
        filename = key.split("/")[-1]
        extension = filename.lower().split(".")[-1]

        # Download file
        obj = s3.get_object(Bucket=bucket, Key=key)
        raw_bytes = obj["Body"].read()
        text_sample = raw_bytes[:5000].decode("utf-8", errors="ignore")

        domain, dataset, optional = extract_name_parts(filename)

        # -------------------------------------------------------
        # CASE 1 — CSV / TSV / TXT
        # -------------------------------------------------------
        if extension in ["csv", "tsv", "txt"]:

            delimiter = "\t" if extension == "tsv" else detect_delimiter(text_sample)

            try:
                df = pd.read_csv(io.BytesIO(raw_bytes), delimiter=delimiter, nrows=50)
                summary = summarize_tabular(df)
                structured = True
                ready = domain is not None and dataset is not None
            except Exception:
                return {
                    "status": "failed",
                    "file_name": filename,
                    "extension": extension,
                    "file_type": "text_unstructured",
                    "structured": False,
                    "ready_for_ingestion": False,
                    "message": "Il file non è tabellare o il delimitatore non è rilevabile."
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
                "structured": structured,
                "columns": list(df.columns),
                "content_summary": summary,
                "ready_for_ingestion": ready,
            }

        # -------------------------------------------------------
        # CASE 2 — JSON or NDJSON
        # -------------------------------------------------------
        if extension in ["json", "ndjson"]:

            # NDJSON → directly ingestable as JSONL
            if extension == "ndjson":
                lines = text_sample.strip().split("\n")
                try:
                    first_line = json.loads(lines[0])
                    summary = summarize_tabular(pd.json_normalize(first_line))
                    ready = domain is not None and dataset is not None
                    return {
                        "status": "success",
                        "file_name": filename,
                        "extension": extension,
                        "file_type": "jsonl",
                        "domain": domain,
                        "dataset": dataset,
                        "name_optional": optional,
                        "structured": True,
                        "columns": list(first_line.keys()),
                        "content_summary": summary,
                        "ready_for_ingestion": ready,
                    }
                except:
                    return {
                        "status": "failed",
                        "file_type": "jsonl_invalid",
                        "message": "NDJSON non valido.",
                        "ready_for_ingestion": False
                    }

            # JSON → attempt structured detection
            try:
                parsed = json.loads(text_sample)
            except:
                return {
                    "status": "failed",
                    "file_type": "json_invalid",
                    "message": "JSON non valido.",
                    "ready_for_ingestion": False
                }

            # JSON ARRAY → convertibile (Q1)
            if isinstance(parsed, list):
                if len(parsed) == 0:
                    return {
                        "status": "warning",
                        "file_type": "json_array_empty",
                        "content_summary": "JSON array vuoto.",
                        "ready_for_ingestion": False
                    }

                if isinstance(parsed[0], dict):
                    summary = summarize_json(parsed)
                    ready = domain is not None and dataset is not None
                    return {
                        "status": "warning",
                        "file_type": "json_array",
                        "message": "JSON array rilevato. Verrà convertito in JSON Lines prima dell'ingestion.",
                        "domain": domain,
                        "dataset": dataset,
                        "name_optional": optional,
                        "structured": True,
                        "columns": list(parsed[0].keys()),
                        "content_summary": summary,
                        "ready_for_ingestion": ready
                    }

                # JSON array non strutturato → no ingestion (Q2)
                return {
                    "status": "failed",
                    "file_type": "json_unstructured_array",
                    "message": "JSON array non tabellare. Il file sarà solo archiviato.",
                    "ready_for_ingestion": False
                }

            # JSON OBJECT STRUTTURATO
            if isinstance(parsed, dict):
                summary = summarize_json(parsed)
                ready = domain is not None and dataset is not None
                return {
                    "status": "success",
                    "file_name": filename,
                    "extension": extension,
                    "file_type": "json_object",
                    "domain": domain,
                    "dataset": dataset,
                    "name_optional": optional,
                    "structured": True,
                    "columns": list(parsed.keys()),
                    "content_summary": summary,
                    "ready_for_ingestion": ready
                }

            # CASE fallback
            return {
                "status": "failed",
                "file_type": "json_unstructured",
                "message": "JSON non strutturato. Ingestion non possibile.",
                "ready_for_ingestion": False
            }

        # -------------------------------------------------------
        # CASE 3 — Excel
        # -------------------------------------------------------
        if extension in ["xlsx", "xls"]:
            try:
                excel_file = pd.ExcelFile(io.BytesIO(raw_bytes))
                sheet_to_use = sheet or excel_file.sheet_names[0]

                if sheet is None:
                    sheet_message = (
                        f"Nessun foglio specificato. Uso il primo foglio: '{sheet_to_use}'."
                    )
                else:
                    sheet_message = f"Foglio specificato: '{sheet_to_use}'."

                df = excel_file.parse(sheet_to_use, nrows=50)
                summary = summarize_tabular(df)
                ready = domain is not None and dataset is not None

                return {
                    "status": "success",
                    "file_name": filename,
                    "extension": extension,
                    "file_type": "excel",
                    "domain": domain,
                    "dataset": dataset,
                    "name_optional": optional,
                    "sheet_used": sheet_to_use,
                    "sheet_message": sheet_message,
                    "structured": True,
                    "columns": list(df.columns),
                    "content_summary": summary,
                    "ready_for_ingestion": ready
                }

            except Exception as e:
                return {
                    "status": "failed",
                    "file_type": "excel_invalid",
                    "message": str(e),
                    "ready_for_ingestion": False
                }

        # -------------------------------------------------------
        # Unsupported extension
        # -------------------------------------------------------
        return {
            "status": "failed",
            "file_name": filename,
            "extension": extension,
            "file_type": "unsupported",
            "message": "Formato non supportato per ingestion.",
            "ready_for_ingestion": False
        }

    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
            "stack_trace": repr(e)
        }
