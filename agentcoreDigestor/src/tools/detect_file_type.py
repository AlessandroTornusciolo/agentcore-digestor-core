import json
import boto3
from strands import tool

lambda_client = boto3.client("lambda")


@tool
def detect_file_type(file_s3_path: str, sheet: str = None) -> dict:
    """
    Wrapper AgentCore per la Lambda detect_file_type.
    """

    payload = {
        "file_s3_path": file_s3_path
    }

    if sheet is not None:
        payload["sheet"] = sheet

    response = lambda_client.invoke(
        FunctionName="agentcore-digestor-lambda-detect-file-type-dev",
        InvocationType="RequestResponse",
        Payload=json.dumps(payload)
    )

    return json.loads(response["Payload"].read().decode("utf-8"))
