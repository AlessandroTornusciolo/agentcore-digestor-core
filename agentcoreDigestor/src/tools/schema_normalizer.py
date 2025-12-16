import json
import boto3
from strands import tool

lambda_client = boto3.client("lambda")


@tool
def schema_normalizer(file_s3_path: str, schema: dict = None, mode: str = "drop_invalid") -> dict:
    payload = {
        "file_s3_path": file_s3_path
    }

    response = lambda_client.invoke(
        FunctionName="agentcore-digestor-lambda-schema-normalizer-dev",
        InvocationType="RequestResponse",
        Payload=json.dumps(payload)
    )

    return json.loads(response["Payload"].read().decode("utf-8"))
