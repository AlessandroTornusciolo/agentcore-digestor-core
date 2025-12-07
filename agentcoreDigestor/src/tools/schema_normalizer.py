import pandas as pd
import numpy as np
from datetime import datetime
from strands import tool
import boto3
import io

s3 = boto3.client("s3")


# ----------------------------------------------------
# Helper: test conversion of a single value
# ----------------------------------------------------
def try_convert(value, dtype):
    try:
        if dtype == "int":
            return int(value)
        elif dtype == "float":
            return float(value)
        elif dtype == "datetime":
            return pd.to_datetime(value)
        return value
    except:
        return None


# ----------------------------------------------------
# Infer best type using >50% majority rule
# ----------------------------------------------------
def infer_column_type(series: pd.Series):
    total = len(series)
    if total == 0:
        return "string"

    counts = {"int": 0, "float": 0, "datetime": 0}

    for v in series:
        # skip None / NaN
        if v is None or pd.isna(v):
            continue

        if try_convert(v, "int") is not None:
            counts["int"] += 1
        if try_convert(v, "float") is not None:
            counts["float"] += 1
        if try_convert(v, "datetime") is not None:
            counts["datetime"] += 1

    best_type = max(counts, key=lambda t: counts[t])

    if counts[best_type] >= total * 0.5:
        return best_type

    return "string"


# ----------------------------------------------------
# Apply conversion according to inferred type
# ----------------------------------------------------
def convert_column(series: pd.Series, dtype: str, mode: str):
    new_values = []
    removed_rows = 0

    for v in series:
        if v is None or pd.isna(v):
            new_values.append(None)
            continue

        converted = try_convert(v, dtype)
        if converted is None:
            if mode == "drop_invalid":
                new_values.append("ROW_REMOVED")
                removed_rows += 1
            else:
                new_values.append(None)
        else:
            new_values.append(converted)

    return new_values, removed_rows


# ----------------------------------------------------
# Main Tool: schema_normalizer
# ----------------------------------------------------
@tool
def schema_normalizer(file_s3_path: str, schema: dict, mode: str = "drop_invalid"):
    """
    Normalizza i dati secondo majority inference (>50%) e converte i valori.
    mode = drop_invalid | keep_nulls
    """

    # --------------------------------------------------------
    # Scarica file da S3
    # --------------------------------------------------------
    bucket = file_s3_path.replace("s3://", "").split("/")[0]
    key = "/".join(file_s3_path.replace("s3://", "").split("/")[1:])

    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))

    original_rows = len(df)
    total_removed = 0

    normalized_df = pd.DataFrame()

    final_schema = {}

    # --------------------------------------------------------
    # Applica inferenza tipo e conversione per ogni colonna
    # --------------------------------------------------------
    for col in df.columns:
        series = df[col].replace({np.nan: None})
        series = series.replace("nan", None)

        inferred_type = infer_column_type(series)
        final_schema[col] = inferred_type

        converted_values, removed = convert_column(series, inferred_type, mode)
        total_removed += removed

        normalized_df[col] = converted_values

    # Rimuovi le righe marcate come ROW_REMOVED
    if mode == "drop_invalid":
        normalized_df = normalized_df[~normalized_df.eq("ROW_REMOVED").any(axis=1)]

    cleaned_rows = len(normalized_df)

    # --------------------------------------------------------
    # Prepara risposta
    # --------------------------------------------------------
    preview = normalized_df.fillna("None").astype(str).head(5).to_dict(orient="records")

    result = {
        "status": "success",
        "mode": mode,
        "schema_normalized": final_schema,
        "rows_original": original_rows,
        "rows_cleaned": cleaned_rows,
        "rows_removed": original_rows - cleaned_rows,
        "ready_for_load": True,
        "sample_preview": preview
    }

    return result
