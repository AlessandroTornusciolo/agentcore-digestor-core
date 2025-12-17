import os
import json
import boto3
import pandas as pd
import numpy as np
import io
import csv as pycsv

s3 = boto3.client("s3")

MAX_SAMPLE_INVALID = int(os.environ.get("MAX_SAMPLE_INVALID", "3"))


def _parse_s3_path(path: str):
    if not path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {path}")
    path = path.replace("s3://", "")
    bucket = path.split("/")[0]
    key = "/".join(path.split("/")[1:])
    if not bucket or not key:
        raise ValueError(f"Invalid S3 path: {path}")
    return bucket, key


def _is_missing(v) -> bool:
    if v is None:
        return True
    if isinstance(v, float) and np.isnan(v):
        return True
    s = str(v).strip()
    return s == "" or s.lower() == "nan" or s.lower() == "none"


def _can_parse_int(v) -> bool:
    try:
        # accetta "1" o "1.0" come int, rifiuta "1.2"
        f = float(str(v).strip())
        return f.is_integer()
    except Exception:
        return False


def _can_parse_float(v) -> bool:
    try:
        float(str(v).strip())
        return True
    except Exception:
        return False


def _can_parse_datetime(v) -> bool:
    try:
        # infer_datetime_format è deprecato, pd.to_datetime gestisce comunque bene
        pd.to_datetime(str(v).strip(), errors="raise")
        return True
    except Exception:
        return False


def _expected_type(t: str) -> str:
    t = (t or "").strip().lower()
    if t in ("int", "integer", "bigint", "smallint"):
        return "int"
    if t in ("float", "double", "decimal", "real"):
        return "float"
    if t in ("datetime", "timestamp", "date"):
        return "datetime"
    return "string"


def _severity(rows_total: int, rows_with_issues: int) -> str:
    if rows_total <= 0:
        return "info"
    ratio = rows_with_issues / rows_total
    if ratio == 0:
        return "info"
    if ratio <= 0.10:
        return "warning"
    if ratio <= 0.30:
        return "warning"
    return "error"


def handler(event, context):
    try:
        file_s3_path = event["file_s3_path"]
        schema = event.get("schema")

        if not schema or not isinstance(schema, list):
            return {"status": "failed", "error": "Missing or invalid 'schema' (expected list of {name,type})."}

        bucket, key = _parse_s3_path(file_s3_path)

        obj = s3.get_object(Bucket=bucket, Key=key)
        raw_bytes = obj["Body"].read()

        # pandas per robustezza; niente scritture, niente conversioni persistenti
        df = pd.read_csv(io.BytesIO(raw_bytes))

        rows_total = int(len(df))
        warnings = []

        # se schema include colonne non presenti -> warning
        # se df include colonne extra -> warning (non bloccante)
        schema_cols = []
        col_types = {}
        for c in schema:
            name = c.get("name") or c.get("column")
            if not name:
                continue
            schema_cols.append(name)
            col_types[name] = _expected_type(c.get("type"))

        extra_cols = [c for c in df.columns if c not in schema_cols]
        if extra_cols:
            warnings.append(f"Input file has extra columns not in schema: {extra_cols}")

        missing_cols = [c for c in schema_cols if c not in df.columns]
        if missing_cols:
            warnings.append(f"Input file is missing schema columns: {missing_cols}")

        columns_report = {}
        row_issue_mask = np.zeros(rows_total, dtype=bool)

        for col in schema_cols:
            expected = col_types.get(col, "string")

            if col not in df.columns:
                columns_report[col] = {
                    "expected_type": expected,
                    "present": False,
                    "null_count": rows_total,
                    "invalid_count": 0,
                    "sample_invalid": [],
                }
                # tutte le righe hanno issue perché colonna mancante
                row_issue_mask |= True
                continue

            s = df[col].replace({np.nan: None})
            null_count = 0
            invalid_count = 0
            sample_invalid = []

            for idx, v in enumerate(s.tolist()):
                if _is_missing(v):
                    null_count += 1
                    row_issue_mask[idx] = True
                    continue

                if expected == "string":
                    # string: tutto ok, nessun controllo
                    continue

                ok = False
                if expected == "int":
                    ok = _can_parse_int(v)
                elif expected == "float":
                    ok = _can_parse_float(v)
                elif expected == "datetime":
                    ok = _can_parse_datetime(v)

                if not ok:
                    invalid_count += 1
                    row_issue_mask[idx] = True
                    if len(sample_invalid) < MAX_SAMPLE_INVALID:
                        sample_invalid.append(str(v))

            columns_report[col] = {
                "expected_type": expected,
                "present": True,
                "null_count": int(null_count),
                "invalid_count": int(invalid_count),
                "sample_invalid": sample_invalid,
            }

        rows_with_issues = int(row_issue_mask.sum())
        issues_ratio = float(rows_with_issues / rows_total) if rows_total else 0.0

        # sample “issue rows” (solo per diagnosi), senza timestamp/oggetti non serializzabili
        sample_issue_rows = []
        if rows_total:
            issue_indices = np.where(row_issue_mask)[0][:5].tolist()
            for i in issue_indices:
                row = df.iloc[int(i)].to_dict()
                # forza a string/None per sicurezza JSON
                safe_row = {k: (None if _is_missing(v) else str(v)) for k, v in row.items()}
                sample_issue_rows.append(safe_row)

        return {
            "status": "success",
            "file_s3_path": file_s3_path,
            "rows_total": rows_total,
            "rows_with_issues": rows_with_issues,
            "issues_ratio": round(issues_ratio, 4),
            "columns": columns_report,
            "warnings": warnings,
            "severity": _severity(rows_total, rows_with_issues),
            "safe_to_normalize": True,  # normalize deciderà drop/keep
            "sample_issue_rows": sample_issue_rows,
        }

    except Exception as e:
        return {"status": "failed", "error": str(e), "stack_trace": repr(e)}
