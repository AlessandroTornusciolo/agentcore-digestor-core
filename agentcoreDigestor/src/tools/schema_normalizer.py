import pandas as pd
import numpy as np
from strands import tool
import boto3
import io

s3 = boto3.client("s3")

NORMALIZED_BUCKET = "agentcore-digestor-upload-raw-dev"
NORMALIZED_PREFIX = "normalized"


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


def infer_column_type(series: pd.Series):
    total = len(series)
    if total == 0:
        return "string"

    counts = {"int": 0, "float": 0, "datetime": 0}

    for v in series:
        if v is None or pd.isna(v):
            continue

        if try_convert(v, "int") is not None:
            counts["int"] += 1
        if try_convert(v, "float") is not None:
            counts["float"] += 1
        if try_convert(v, "datetime") is not None:
            counts["datetime"] += 1

    best = max(counts, key=lambda t: counts[t])
    return best if counts[best] >= total * 0.5 else "string"


@tool
def schema_normalizer(file_s3_path: str, schema: dict, mode: str = "drop_invalid"):
    """
    Row-level normalization.
    A row is valid ONLY if all columns are convertible.
    """

    # Load CSV
    bucket = file_s3_path.replace("s3://", "").split("/")[0]
    key = "/".join(file_s3_path.replace("s3://", "").split("/")[1:])
    df = pd.read_csv(io.BytesIO(s3.get_object(Bucket=bucket, Key=key)["Body"].read()))

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

            # MISSING VALUE = INVALID ROW
            if pd.isna(value) or value == "" or value == "nan":
                row_valid = False
                break

            converted = try_convert(value, dtype)
            if converted is None:
                row_valid = False
                break

            new_row[col] = converted

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
