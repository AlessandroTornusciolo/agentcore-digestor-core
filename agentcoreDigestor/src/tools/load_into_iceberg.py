import json
import boto3
from strands import tool

lambda_client = boto3.client("lambda")


@tool
def load_into_iceberg(file_s3_path: str, table_name: str, schema: list = None) -> dict:
    """
    Tool che inoltra il lavoro alla Lambda dockerizzata 'load_into_iceberg'.
    Non esegue alcun parsing del file.
    """

    payload = {
        "file_s3_path": file_s3_path,
        "table_name": table_name,
        "schema": schema  # non obbligatorio, ma utile per future estensioni
    }

    response = lambda_client.invoke(
        FunctionName="agentcore-digestor-lambda-load-into-iceberg-dev",
        InvocationType="RequestResponse",
        Payload=json.dumps(payload)
    )

    return json.loads(response["Payload"].read().decode("utf-8"))
