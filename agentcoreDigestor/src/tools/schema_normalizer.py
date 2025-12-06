from strands import tool
import pandas as pd
import boto3
import io
from datetime import datetime

s3 = boto3.client("s3")


def _read_file(file_s3_path: str) -> pd.DataFrame:
    """Internal helper: loads CSV or Parquet depending on extension."""
    bucket, key = file_s3_path.replace("s3://", "").split("/", 1)
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()

    if key.endswith(".csv"):
        return pd.read_csv(io.BytesIO(body))
    elif key.endswith(".parquet"):
        return pd.read_parquet(io.BytesIO(body))
    else:
        raise ValueError(f"Unsupported file type: {key}")


def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize col names: lowercase, strip spaces, replace non-std chars."""
    df = df.rename(columns={
        col: col.strip().lower().replace(" ", "_")
        for col in df.columns
    })
    return df


def _safe_to_float(val):
    try:
        return float(val)
    except:
        return None


def _safe_to_int(val):
    try:
        return int(val)
    except:
        return None


def _safe_to_date(val):
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(str(val), fmt).date()
        except:
            pass
    return None


@tool
def schema_normalizer(
    file_s3_path: str,
    mode: str = "keep_nulls"
) -> dict:
    """
    Normalizes schema and values.

    Parameters
    ----------
    file_s3_path : str
        S3 path to CSV/Parquet
    mode : str
        "keep_nulls" -> invalid values converted to None
        "drop_invalid" -> rows with invalid data are removed

    Returns
    -------
    dict:
        {
            status: "success",
            schema: [...],
            normalization_actions: [...],
            warnings: [...],
            cleaned_row_count: int,
            original_row_count: int,
            recommendation: "ready_for_load" | "check_issues"
        }
    """
    df = _read_file(file_s3_path)
    original_count = len(df)

    actions = []
    warnings = []

    # ----------------------------------------------
    # 1. Normalize column names
    # ----------------------------------------------
    df = _normalize_column_names(df)
    actions.append("Normalized column names (lowercase, underscores).")

    # ----------------------------------------------
    # 2. Detect and convert datatypes
    # ----------------------------------------------
    df_clean = df.copy()
    invalid_rows = set()

    for col in df_clean.columns:
        series = df_clean[col]

        # Try numeric
        if series.dtype == object:
            # heuristic: check % of numeric-looking values
            numeric_ratio = series.apply(
                lambda x: str(x).replace(".", "", 1).isdigit()
            ).mean()

            if numeric_ratio > 0.6:
                # try convert to float
                df_clean[col] = series.apply(_safe_to_float)
                actions.append(f"Converted column '{col}' to float.")
                # mark invalid
                bad_idx = df_clean[df_clean[col].isnull()].index
                invalid_rows.update(bad_idx)

        # Try date
        if series.dtype == object and "date" in col:
            df_clean[col] = series.apply(_safe_to_date)
            actions.append(f"Parsed column '{col}' as date.")
            bad_idx = df_clean[df_clean[col].isnull()].index
            invalid_rows.update(bad_idx)

    # ----------------------------------------------
    # 3. Apply mode
    # ----------------------------------------------
    invalid_rows = list(invalid_rows)
    removed_count = 0

    if mode == "drop_invalid" and invalid_rows:
        df_clean = df_clean.drop(index=invalid_rows)
        removed_count = len(invalid_rows)
        warnings.append({"dropped_rows": removed_count})
        actions.append(f"Removed {removed_count} invalid rows.")
    elif mode == "keep_nulls" and invalid_rows:
        warnings.append({"nullified_rows": len(invalid_rows)})
        actions.append("Replaced invalid values with nulls (kept rows).")

    cleaned_count = len(df_clean)

    # ----------------------------------------------
    # 4. Build schema description
    # ----------------------------------------------
    schema = [{"name": c, "type": str(df_clean[c].dtype)} for c in df_clean.columns]

    return {
        "status": "success",
        "schema": schema,
        "normalization_actions": actions,
        "warnings": warnings,
        "original_row_count": original_count,
        "cleaned_row_count": cleaned_count,
        "recommendation": "ready_for_load" if not warnings else "review_warnings",
    }
