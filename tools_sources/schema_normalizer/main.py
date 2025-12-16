import pandas as pd
import numpy as np
import boto3
import io
import os

s3 = boto3.client("s3")

NORMALIZED_BUCKET = os.environ.get(
    "NORMALIZED_BUCKET",
    "agentcore-digestor-upload-raw-dev"
)
NORMALIZED_PREFIX = "normalized"


# --------------------------------------------------
# Conversion helpers
# --------------------------------------------------
def can_convert_int(v):
    try:
        i = int(v)
        return float(v) == float(i)
    except:
        return False


def can_convert_float(v):
    try:
        float(v)
        return True
    except:
        return False


def can_convert_datetime(v):
    try:
        pd.to_datetime(v)
        return True
    except:
        return False


def convert_value(v, dtype):
    if dtype == "int":
        return int(v)
    if dtype == "float":
        return float(v)
    if dtype == "datetime":
        return pd.to_datetime(v)
    return v


# --------------------------------------------------
# Schema inference (STRICT & SAFE)
# --------------------------------------------------
def infer_column_type(series: pd.Series):
    values = [v for v in series if v is not None and not pd.isna(v)]

    if not values:
        return "string"

    total = len(values)

    # -----------------------------
    # DATETIME (solo stringhe)
    # -----------------------------
    datetime_ok = [
        v for v in values
        if isinstance(v, str) and can_convert_datetime(v)
    ]
    if len(datetime_ok) / total >= 0.6:
        return "datetime"

    # -----------------------------
    # NUMERIC
    # -----------------------------
    float_ok = [v for v in values if can_convert_float(v)]
    if len(float_ok) / total >= 0.6:
        if all(can_convert_int(v) for v in float_ok):
            return "int"
        return "float"

    return "string"


# --------------------------------------------------
# Make DataFrame JSON-safe (Lambda response)
# --------------------------------------------------
def json_safe_df(df: pd.DataFrame) -> pd.DataFrame:
    safe = df.copy()
    for col in safe.columns:
        if pd.api.types.is_datetime64_any_dtype(safe[col]):
            safe[col] = safe[col].astype(str)
    return safe


# --------------------------------------------------
# Lambda handler
# --------------------------------------------------
def handler(event, context):
    try:
        file_s3_path = event["file_s3_path"]

        bucket = file_s3_path.replace("s3://", "").split("/")[0]
        key = "/".join(file_s3_path.replace("s3://", "").split("/")[1:])

        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(io.BytesIO(obj["Body"].read()))

        original_rows = len(df)

        # --------------------------------------------------
        # Infer schema
        # --------------------------------------------------
        inferred_schema = {}
        for col in df.columns:
            series = df[col].replace({np.nan: None}).replace("nan", None)
            inferred_schema[col] = infer_column_type(series)

        # --------------------------------------------------
        # Row-level normalization
        # --------------------------------------------------
        cleaned_rows = []
        removed_rows = 0

        for _, row in df.iterrows():
            new_row = {}
            valid = True

            for col, dtype in inferred_schema.items():
                value = row[col]

                # missing value = invalid row
                if value is None or pd.isna(value) or value == "" or value == "nan":
                    valid = False
                    break

                try:
                    new_row[col] = convert_value(value, dtype)
                except:
                    valid = False
                    break

            if valid:
                cleaned_rows.append(new_row)
            else:
                removed_rows += 1

        normalized_df = pd.DataFrame(cleaned_rows)

        # --------------------------------------------------
        # Write normalized CSV (SOURCE OF TRUTH)
        # --------------------------------------------------
        filename = key.split("/")[-1].rsplit(".", 1)[0]
        normalized_key = f"{NORMALIZED_PREFIX}/{filename}_normalized.csv"

        s3.put_object(
            Bucket=NORMALIZED_BUCKET,
            Key=normalized_key,
            Body=normalized_df.to_csv(index=False).encode("utf-8")
        )

        # --------------------------------------------------
        # Prepare JSON-safe preview
        # --------------------------------------------------
        preview_df = json_safe_df(normalized_df)

        return {
            "status": "success",
            "schema_normalized": inferred_schema,
            "rows_original": original_rows,
            "rows_cleaned": len(normalized_df),
            "rows_removed": removed_rows,
            "normalized_path": f"s3://{NORMALIZED_BUCKET}/{normalized_key}",
            "ready_for_load": True,
            "sample_preview": preview_df.head(5).to_dict(orient="records"),
        }

    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
            "stack_trace": repr(e)
        }
