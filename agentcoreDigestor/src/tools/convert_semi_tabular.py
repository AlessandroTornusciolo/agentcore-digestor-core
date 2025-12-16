import json
import boto3
from strands import tool

lambda_client = boto3.client("lambda")

@tool
def convert_semi_tabular(file_s3_path: str, file_type: str, sheet: int = 0) -> dict:
    payload = {
        "file_s3_path": file_s3_path,
        "file_type": file_type,
        "sheet": sheet
    }

    resp = lambda_client.invoke(
        FunctionName="agentcore-digestor-lambda-convert-semi-tabular-dev",
        InvocationType="RequestResponse",
        Payload=json.dumps(payload)
    )

    return json.loads(resp["Payload"].read())
