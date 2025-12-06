import json
import boto3
import datetime
import os

lambda_client = boto3.client("lambda")

AGENT_VERSION = "1.0"


# ------------------------------------------------------------
# Helper: invoke a lambda tool synchronously
# ------------------------------------------------------------
def invoke_tool(function_name, payload):
    try:
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload).encode("utf-8")
        )

        raw = response["Payload"].read().decode("utf-8")
        return json.loads(raw)

    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
            "stack_trace": repr(e)
        }


# ------------------------------------------------------------
# Main Lambda Handler
# ------------------------------------------------------------
def handler(event, context):

    # Extract input body (if API Gateway)
    body = event.get("body")
    if isinstance(body, str):
        body = json.loads(body)

    file_s3_path = body.get("file_s3_path")
    domain       = body.get("domain")
    dataset      = body.get("dataset")
    table_name   = body.get("table_name")
    mode         = body.get("mode", "append")
    options      = body.get("options", {})

    env = os.environ.get("ENV", "dev")

    # Auto-generate table_name if not provided
    if not table_name:
        table_name = f"icg_{domain}_{dataset}_{env}"

    # Final orchestration response structure
    result = {
        "status": "success",
        "steps": {
            "schema": {},
            "validation": {},
            "load": {},
            "schema_normalizer": {},
            "iceberg_ctas": {}
        },
        "warnings": [],
        "errors": [],
        "records_loaded": 0,
        "metadata": {
            "agent_version": AGENT_VERSION,
            "timestamp_utc": datetime.datetime.utcnow().isoformat()
        }
    }

    # ------------------------------------------------------------
    # STEP 1 – analyze_file_schema
    # ------------------------------------------------------------
    analyze_payload = {
        "file_s3_path": file_s3_path,
        "options": options
    }

    schema_result = invoke_tool(
        f"agentcore-digestor-lambda-analyze-schema-{env}",
        analyze_payload
    )
    result["steps"]["schema"] = schema_result

    if schema_result.get("status") != "success":
        result["status"] = "failed"
        result["errors"].append({"schema": schema_result})
        return finalize(result)


    # ------------------------------------------------------------
    # STEP 2 – validate_data
    # ------------------------------------------------------------
    validate_payload = {
        "file_s3_path": file_s3_path,
        "schema": schema_result.get("schema")
    }

    val_result = invoke_tool(
        f"agentcore-digestor-lambda-validate-data-{env}",
        validate_payload
    )
    result["steps"]["validation"] = val_result

    if val_result.get("status") != "success":
        result["status"] = "failed"
        result["errors"].append({"validation": val_result})
        return finalize(result)


    # ------------------------------------------------------------
    # STEP 3 – load_into_iceberg (writes the Parquet files)
    # ------------------------------------------------------------
    load_payload = {
        "file_s3_path": file_s3_path,
        "table_name": table_name
    }

    load_result = invoke_tool(
        f"agentcore-digestor-lambda-load-into-iceberg-{env}",
        load_payload
    )
    result["steps"]["load"] = load_result

    if load_result.get("status") != "success":
        result["status"] = "failed"
        result["errors"].append({"load": load_result})
        return finalize(result)

    result["records_loaded"] = load_result.get("records_loaded", 0)


    # ------------------------------------------------------------
    # STEP 3.5 – schema_normalizer
    #   Determines REAL schema from Parquet files just written
    # ------------------------------------------------------------
    norm_payload = {
        "table_name": table_name
    }

    norm_result = invoke_tool(
        f"agentcore-digestor-lambda-schema-normalizer-{env}",
        norm_payload
    )
    result["steps"]["schema_normalizer"] = norm_result

    if norm_result.get("status") != "success":
        result["status"] = "failed"
        result["errors"].append({"schema_normalizer": norm_result})
        return finalize(result)

    normalized_schema = norm_result.get("normalized_schema")


    # ------------------------------------------------------------
    # STEP 4 – iceberg_ctas (CTAS → Managed Iceberg table)
    # ------------------------------------------------------------
    ctas_payload = {
        "table_name": table_name,
        "schema": normalized_schema
    }

    ctas_result = invoke_tool(
        f"agentcore-digestor-lambda-iceberg-ctas-{env}",
        ctas_payload
    )
    result["steps"]["iceberg_ctas"] = ctas_result

    if ctas_result.get("status") != "success":
        result["status"] = "failed"
        result["errors"].append({"iceberg_ctas": ctas_result})
        return finalize(result)


    # ------------------------------------------------------------
    # FINAL OUTPUT
    # ------------------------------------------------------------
    return finalize(result)



# ------------------------------------------------------------
# HTTP Response wrapper
# ------------------------------------------------------------
def finalize(result):
    return {
        "statusCode": 200,
        "body": json.dumps(result),
        "headers": {"Content-Type": "application/json"}
    }
