import pandas as pd
import numpy as np
from strands import tool
import boto3
import io

s3 = boto3.client("s3")

NORMALIZED_BUCKET = "agentcore-digestor-upload-raw-dev"
NORMALIZED_PREFIX = "normalized"


# --------------------------------------------------
# Safe conversion helpers
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
# CORRECT type inference (float > int)
# --------------------------------------------------
def infer_column_type(series: pd.Series):
    values = [v for v in series if v is not None and not pd.isna(v)]

    if not values:
        return "string"

    # 1. datetime only if ALL values are datetime
    if all(can_convert_datetime(v) for v in values):
        return "datetime"

    # 2. numeric?
    numeric_values = [v for v in values if can_convert_float(v)]
    if len(numeric_values) == len(values):
        # check if all are integers
        if all(can_convert_int(v) for v in numeric_values):
            return "int"
        return "float"

    return "string"


# --------------------------------------------------
# MAIN TOOL
# --------------------------------------------------
@tool
def schema_normalizer(file_s3_path: str, schema: dict, mode: str = "drop_invalid"):
    """
    Row-level normalization.
    A row is valid ONLY if all columns are convertible.
    """

    # Load CSV
    bucket = file_s3_path.replace("s3://", "").split("/")[0]
    key = "/".join(file_s3_path.replace("s3://", "").split("/")[1:])

    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))

    original_rows = len(df)

    # Infer schema
    inferred_schema = {}
    for col in df.columns:
        series = df[col].replace({np.nan: None}).replace("nan", None)
        inferred_schema[col] = infer_column_type(series)

    cleaned_rows = []
    removed_rows = 0

    for _, row in df.iterrows():
        new_row = {}
        row_valid = True

        for col, dtype in inferred_schema.items():
            value = row[col]

            if value is None or pd.isna(value) or value == "" or value == "nan":
                row_valid = False
                break

            try:
                new_row[col] = convert_value(value, dtype)
            except:
                row_valid = False
                break

        if row_valid:
            cleaned_rows.append(new_row)
        else:
            removed_rows += 1

    normalized_df = pd.DataFrame(cleaned_rows)

    # Write normalized CSV
    filename = file_s3_path.split("/")[-1].rsplit(".", 1)[0]
    normalized_key = f"{NORMALIZED_PREFIX}/{filename}_normalized.csv"

    s3.put_object(
        Bucket=NORMALIZED_BUCKET,
        Key=normalized_key,
        Body=normalized_df.to_csv(index=False).encode("utf-8")
    )

    return {
        "status": "success",
        "schema_normalized": inferred_schema,
        "rows_original": original_rows,
        "rows_cleaned": len(normalized_df),
        "rows_removed": removed_rows,
        "normalized_path": f"s3://{NORMALIZED_BUCKET}/{normalized_key}",
        "ready_for_load": True,
        "sample_preview": normalized_df.head(5).to_dict(orient="records"),
    }
