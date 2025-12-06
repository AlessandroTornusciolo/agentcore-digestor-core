import json
import boto3
import csv
import io

s3 = boto3.client("s3")


def validate_row(row, schema):
    """
    Ritorna una lista di errori per la riga.
    """
    errors = []

    for col in schema:
        name = col["name"]
        expected_type = col["type"]
        value = row.get(name)

        # Valori vuoti non sono considerati errori
        if value is None or value == "":
            continue

        try:
            if expected_type == "int":
                int(value)
            elif expected_type == "float":
                float(value)
            elif expected_type == "bool":
                if value.lower() not in ("true", "false"):
                    raise ValueError("Not bool")
        except Exception:
            errors.append(f"Column '{name}' expected {expected_type}, got '{value}'")

    return errors


def handler(event, context):
    try:
        file_s3_path = event["file_s3_path"]
        schema       = event["schema"]

        bucket = file_s3_path.replace("s3://", "").split("/")[0]
        key    = "/".join(file_s3_path.replace("s3://", "").split("/")[1:])

        # Read file
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj["Body"].read().decode("utf-8")

        rows = list(csv.DictReader(io.StringIO(data)))

        warnings = []
        errors = []
        row_count = 0

        for row in rows:
            row_count += 1
            row_errors = validate_row(row, schema)
            if row_errors:
                errors.extend(row_errors)

        status = "success" if not errors else "failed"

        return {
            "status": status,
            "warnings": warnings,
            "errors": errors,
            "row_count": row_count
        }

    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
            "stack_trace": repr(e)
        }
