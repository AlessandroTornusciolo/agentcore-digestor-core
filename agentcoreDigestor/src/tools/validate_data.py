import json
import boto3
from strands import tool

lambda_client = boto3.client("lambda")

LAMBDA_NAME = "agentcore-digestor-lambda-validate-data-dev"


@tool
def validate_data(file_s3_path: str, schema: list) -> dict:
    """
    Diagnostic-only validation:
    - Reads CSV from S3
    - Checks nulls/invalid values against provided schema
    - Does NOT transform or write files
    """
    payload = {
        "file_s3_path": file_s3_path,
        "schema": schema,
    }

    response = lambda_client.invoke(
        FunctionName=LAMBDA_NAME,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload),
    )

    return json.loads(response["Payload"].read().decode("utf-8"))
