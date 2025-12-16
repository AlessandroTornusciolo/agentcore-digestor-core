import json
import boto3
from strands import tool

lambda_client = boto3.client("lambda")


@tool
def analyze_schema(file_s3_path: str, file_format: str, max_rows: int = 50) -> dict:
    """
    Delegates schema analysis to the analyze_schema Lambda.
    """

    payload = {
        "file_s3_path": file_s3_path,
        "file_format": file_format,
        "max_rows": max_rows
    }

    response = lambda_client.invoke(
        FunctionName="agentcore-digestor-lambda-analyze-schema-dev",
        InvocationType="RequestResponse",
        Payload=json.dumps(payload)
    )

    return json.loads(response["Payload"].read().decode("utf-8"))
