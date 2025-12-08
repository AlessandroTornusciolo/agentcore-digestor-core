import boto3
import os
from datetime import datetime
from strands import tool

s3 = boto3.client("s3")


@tool
def raw_ingest(file_s3_path: str) -> dict:
    """
    Copies the raw source file into the RAW archive bucket.

    Path structure:
        s3://agentcore-digestor-archive-<env>/<extension>/<YYYY-MM-DD>/<filename>

    This tool:
    - DOES NOT restrict on extension (pdf, docx, md, ecc. OK)
    - Always archives the file if the S3 path is valid
    - Returns structured metadata for downstream tools / reasoning
    """

    # -------------------------------------------------------------
    # Extract filename & extension
    # -------------------------------------------------------------
    filename = file_s3_path.split("/")[-1]

    if "." in filename:
        extension = filename.split(".")[-1].lower()
    else:
        extension = "unknown"

    # -------------------------------------------------------------
    # Extract bucket/key from S3 path
    # -------------------------------------------------------------
    original_bucket = file_s3_path.replace("s3://", "").split("/")[0]
    original_key = "/".join(file_s3_path.replace("s3://", "").split("/")[1:])

    # -------------------------------------------------------------
    # Prepare archive bucket & path
    # -------------------------------------------------------------
    env = os.environ.get("ENV", "dev")
    archive_bucket = f"agentcore-digestor-archive-{env}"

    today = datetime.utcnow().strftime("%Y-%m-%d")
    archive_key = f"{extension}/{today}/{filename}"

    # -------------------------------------------------------------
    # Copy file into archive bucket
    # -------------------------------------------------------------
    s3.copy_object(
        Bucket=archive_bucket,
        CopySource={"Bucket": original_bucket, "Key": original_key},
        Key=archive_key
    )

    # -------------------------------------------------------------
    # Return useful structured metadata
    # -------------------------------------------------------------
    return {
        "status": "success",
        "archive_path": f"s3://{archive_bucket}/{archive_key}",
        "file_extension": extension,
        "original_path": file_s3_path,
        "env": env,
    }
