import json
import boto3
from strands import tool

lambda_client = boto3.client("lambda")


@tool
def load_into_iceberg(file_s3_path: str, table_name: str, schema: list) -> dict:
    """
    Tool per orchestrare il caricamento dei dati nella tabella Iceberg.
    NON esegue il caricamento locale, ma invoca la Lambda dedicata.

    Args:
        file_s3_path: path S3 del file normalizzato da caricare
        table_name: tabella iceberg di destinazione
        schema: schema normalizzato proveniente da schema_normalizer

    Returns:
        Risultato della Lambda load_into_iceberg
    """

    payload = {
        "file_s3_path": file_s3_path,
        "table_name": table_name,
        "schema": schema   # può servire alle lambda future
    }

    response = lambda_client.invoke(
        FunctionName="agentcore-digestor-lambda-load-into-iceberg-dev",
        InvocationType="RequestResponse",
        Payload=json.dumps(payload)
    )

    return json.loads(response["Payload"].read().decode("utf-8"))
