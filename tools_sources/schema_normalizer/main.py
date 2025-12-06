import os
import json
import boto3
import time
from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq

s3 = boto3.client("s3")


# ---------------------------------------------
# Helpers: type normalization & merge
# ---------------------------------------------
def arrow_type_to_glue(arrow_type: pa.DataType) -> str:
    """
    Converte un tipo PyArrow in un tipo compatibile con Glue/Athena/Iceberg.
    """
    if pa.types.is_int8(arrow_type) or pa.types.is_int16(arrow_type) or pa.types.is_int32(arrow_type):
        return "int"
    if pa.types.is_int64(arrow_type):
        return "bigint"

    if pa.types.is_float16(arrow_type) or pa.types.is_float32(arrow_type):
        return "float"
    if pa.types.is_float64(arrow_type):
        return "double"

    if pa.types.is_boolean(arrow_type):
        return "boolean"

    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type) or pa.types.is_binary(arrow_type):
        return "string"

    if pa.types.is_timestamp(arrow_type):
        # Potresti specializzare su unità/timezone se ti serve
        return "timestamp"

    if pa.types.is_date32(arrow_type) or pa.types.is_date64(arrow_type):
        return "date"

    if pa.types.is_decimal(arrow_type):
        # decimal128/256
        return f"decimal({arrow_type.precision},{arrow_type.scale})"

    if pa.types.is_list(arrow_type):
        value_type = arrow_type.value_type
        return f"array<{arrow_type_to_glue(value_type)}>"

    if pa.types.is_struct(arrow_type):
        fields = []
        for field in arrow_type:
            ftype = arrow_type_to_glue(field.type)
            fields.append(f"{field.name}:{ftype}")
        inner = ",".join(fields)
        return f"struct<{inner}>"

    if pa.types.is_map(arrow_type):
        key_type = arrow_type_to_glue(arrow_type.key_type)
        item_type = arrow_type_to_glue(arrow_type.item_type)
        return f"map<{key_type},{item_type}>"

    # Fallback generico
    return "string"


def merge_glue_types(t1: str, t2: str) -> str:
    """
    Merge "furbo" di due tipi Glue/Athena in uno solo compatibile.
    Regole semplici per ora; puoi espanderle in futuro.
    """
    if t1 == t2:
        return t1

    numeric = {"tinyint", "smallint", "int", "bigint", "float", "double", "decimal"}
    if t1 in numeric and t2 in numeric:
        # Se uno è double, vince double
        if "double" in (t1, t2):
            return "double"
        if "float" in (t1, t2):
            return "float"
        if "bigint" in (t1, t2):
            return "bigint"
        return "int"

    # Timestamp vs string → tieni timestamp
    if (t1 == "timestamp" and t2 == "string") or (t2 == "timestamp" and t1 == "string"):
        return "timestamp"

    # Per tutto il resto: fallback string
    return "string"


def merge_schemas(schema_list):
    """
    Receives a list of PyArrow schemas and merges them into a single
    dict: { column_name: arrow_type }
    """
    merged = {}

    for schema in schema_list:
        for field in schema:
            name = field.name
            atype = field.type

            if name not in merged:
                merged[name] = atype
            else:
                # Se diverso, decidiamo un super-tipo al livello Glue
                # mappando in Glue e poi "tornando" se serve
                g1 = arrow_type_to_glue(merged[name])
                g2 = arrow_type_to_glue(atype)
                g_merged = merge_glue_types(g1, g2)

                # Non tentiamo un "ritorno" a arrow_type, ci basta il tipo Glue.
                # Manteniamo un dummy arrow_type coerente con g_merged? Non serve,
                # ci interessa solo il tipo Glue finale.
                # Quindi salviamo g_merged in un wrapper speciale:
                merged[name] = g_merged

    # A questo punto merged può contenere DataType o string (Glue-type).
    # Normalizziamo tutto in Glue-type string.
    glue_schema = {}
    for name, value in merged.items():
        if isinstance(value, pa.DataType):
            glue_schema[name] = arrow_type_to_glue(value)
        else:
            glue_schema[name] = value

    return glue_schema


# ---------------------------------------------
# S3 helpers
# ---------------------------------------------
def list_parquet_keys(bucket, prefix, max_files=10):
    keys = []
    continuation = None

    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if continuation:
            kwargs["ContinuationToken"] = continuation

        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            if obj["Key"].endswith(".parquet") or obj["Key"].endswith(".snappy.parquet"):
                keys.append(obj["Key"])
                if len(keys) >= max_files:
                    return keys

        if resp.get("IsTruncated"):
            continuation = resp.get("NextContinuationToken")
        else:
            break

    return keys


def read_schema_from_parquet_s3(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    bio = BytesIO(body)

    pf = pq.ParquetFile(bio)
    return pf.schema_arrow


# ---------------------------------------------
# Lambda handler
# ---------------------------------------------
def handler(event, context):
    """
    Event atteso:
    {
      "table_name": "icg_demo_sample_dev",
      "max_files": 10     # opzionale
    }
    """
    try:
        env = os.environ.get("ENV", "dev")

        table_name = event.get("table_name")
        if not table_name:
            return {
                "status": "failed",
                "error": "Missing 'table_name' in event"
            }

        max_files = event.get("max_files", 10)

        bucket = f"agentcore-digestor-tables-{env}"
        prefix = f"{table_name}/data/"

        # 1. Lista dei Parquet da campionare
        keys = list_parquet_keys(bucket, prefix, max_files=max_files)
        if not keys:
            return {
                "status": "failed",
                "error": f"No Parquet files found under s3://{bucket}/{prefix}"
            }

        # 2. Legge schema da un subset di file
        schemas = []
        for key in keys:
            try:
                sch = read_schema_from_parquet_s3(bucket, key)
                schemas.append(sch)
            except Exception as e:
                # Accumula warning ma continua
                # Potresti in futuro loggare su CloudWatch in modo più dettagliato
                pass

        if not schemas:
            return {
                "status": "failed",
                "error": "Could not read schema from any sampled Parquet file"
            }

        # 3. Merge + normalizzazione in tipi Glue
        glue_schema = merge_schemas(schemas)

        # 4. Risultato finale
        normalized_schema = [
            {"name": col_name, "type": col_type}
            for col_name, col_type in glue_schema.items()
        ]

        return {
            "status": "success",
            "table_name": table_name,
            "bucket": bucket,
            "prefix": prefix,
            "sampled_files": keys,
            "normalized_schema": normalized_schema
        }

    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
            "stack_trace": repr(e)
        }
