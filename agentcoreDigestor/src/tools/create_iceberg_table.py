import json
import boto3
from strands import tool

lambda_client = boto3.client("lambda")

# ---------------------------------------------------------
# Mapping tool-types → Glue types
# ---------------------------------------------------------
TYPE_MAP = {
    "int": "int",
    "float": "double",
    "string": "string",
    "datetime": "timestamp"
}

def convert_schema_for_glue(schema: dict):
    """
    Convert schema from tool types to Glue-compatible types.
    Input format: {"colname": "int", ...}
    Output format: [{"name": "...", "type": "int"}, ...]
    """
    converted = []
    for col, dtype in schema.items():
        glue_type = TYPE_MAP.get(dtype, "string")
        converted.append({"name": col, "type": glue_type})
    return converted


# ---------------------------------------------------------
# Tool: create_iceberg_table
# ---------------------------------------------------------
@tool
def create_iceberg_table(table_name: str, schema: dict) -> dict:
    """
    Calls the iceberg_ctas Lambda with schema converted to Glue types.
    """
    
    # 1) convert schema
    glue_schema = convert_schema_for_glue(schema)

    payload = {
        "table_name": table_name,
        "schema": glue_schema
    }

    response = lambda_client.invoke(
        FunctionName="agentcore-digestor-lambda-iceberg-ctas-dev",
        InvocationType="RequestResponse",
        Payload=json.dumps(payload)
    )

    return json.loads(response["Payload"].read().decode("utf-8"))
