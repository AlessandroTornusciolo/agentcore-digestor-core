import boto3
import pandas as pd

from io import StringIO
from botocore.exceptions import ClientError
from strands import tool


def _read_csv_from_s3(file_s3_path: str) -> pd.DataFrame:
    """
    Internal helper: reads a CSV from S3 into a DataFrame.
    """
    if not file_s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {file_s3_path}")

    _, _, bucket_and_key = file_s3_path.partition("s3://")
    bucket, _, key = bucket_and_key.partition("/")

    s3 = boto3.client("s3")

    obj = s3.get_object(Bucket=bucket, Key=key)
    csv_data = obj["Body"].read().decode("utf-8")

    return pd.read_csv(StringIO(csv_data))


def _normalize_column(df: pd.DataFrame, col: str, col_type: str) -> None:
    """
    Convert a column to the target type, in-place.
    Supports basic types: int, float, date/datetime, string.
    """
    # Normalize type name (from analyze_schema)
    t = (col_type or "").lower()

    if "int" in t:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    elif "float" in t or "double" in t or "decimal" in t:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    elif "date" in t or "time" in t:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    else:
        # treat as generic string
        df[col] = df[col].astype(str)


@tool
def validate_data(
    file_s3_path: str,
    schema: list,
    mode: str = "drop_invalid"
) -> dict:
    """
    Validate and normalize a CSV file according to the inferred schema.

    Parameters
    ----------
    file_s3_path : str
        S3 URI of the CSV file.
    schema : list
        List of {name, type} dictionaries from analyze_schema().
    mode : str
        Validation strategy:
        - "drop_invalid": remove rows with invalid values in any typed column
        - "keep_nulls": keep rows, invalid values become null (NaN)

    Returns
    -------
    dict
        {
            status: "success" | "failed",
            original_rows: int,
            valid_rows: int,
            removed_rows: int,
            normalized_schema: [...],
            warnings: [...],
            sample: [...],
            mode_used: str
        }
    """
    try:
        df = _read_csv_from_s3(file_s3_path)
    except (ValueError, ClientError) as e:
        return {
            "status": "failed",
            "error": f"Could not read S3 file: {str(e)}"
        }

    original_rows = len(df)
    warnings = []
    normalized_cols = {}

    # ---------------------------------------------------------
    # STEP 1 — Ensure all schema columns exist and convert types
    # ---------------------------------------------------------
    for col_def in schema:
        col_name = col_def.get("name")
        col_type = col_def.get("type")

        if not col_name:
            continue

        if col_name not in df.columns:
            warnings.append(f"Column '{col_name}' missing in input file; created as null.")
            df[col_name] = None

        _normalize_column(df, col_name, col_type)
        normalized_cols[col_name] = col_type

    # ---------------------------------------------------------
    # STEP 2 — Handle invalid values according to mode
    # ---------------------------------------------------------
    before = len(df)

    if mode == "drop_invalid":
        # drop rows where ANY of the normalized columns is null
        df = df.dropna(subset=list(normalized_cols.keys()), how="any")
        removed = before - len(df)
    else:
        # keep_nulls: don't drop, just count how many missing cells abbiamo
        removed = int(df[list(normalized_cols.keys())].isna().sum().sum())

    valid_rows = len(df)

    # ---------------------------------------------------------
    # STEP 3 — Build final response
    # ---------------------------------------------------------
    normalized_schema = [
        {"column": col, "type": normalized_cols[col]}
        for col in normalized_cols
    ]

    sample_rows = df.head(5).to_dict(orient="records")

    return {
        "status": "success",
        "original_rows": original_rows,
        "valid_rows": valid_rows,
        "removed_rows": removed,
        "normalized_schema": normalized_schema,
        "warnings": warnings,
        "sample": sample_rows,
        "mode_used": mode
    }
