import boto3
import os
import datetime
from strands import tool

s3 = boto3.client("s3")

ARCHIVE_BUCKET = "agentcore-digestor-archive-dev"

SUPPORTED_EXT = {"csv", "tsv", "json", "ndjson", "xlsx", "xls", "txt"}


@tool
def raw_ingest(file_s3_path: str) -> dict:
    """
    Copies the raw source file into the RAW archive bucket.
    Path structure:
        s3://agentcore-digestor-archive-dev/<extension>/<YYYY-MM-DD>/<filename>

    Returns metadata needed for the ingestion pipeline.
    """

    # -------------------------------------------------------------
    # Validate extension
    # -------------------------------------------------------------
    filename = file_s3_path.split("/")[-1]
    extension = filename.split(".")[-1].lower()

    if extension not in SUPPORTED_EXT:
        return {
            "status": "failed",
            "error": f"Unsupported file extension: {extension}",
            "supported_extensions": list(SUPPORTED_EXT)
        }

    # -------------------------------------------------------------
    # Extract bucket/key from S3 path
    # -------------------------------------------------------------
    original_bucket = file_s3_path.replace("s3://", "").split("/")[0]
    original_key = "/".join(file_s3_path.replace("s3://", "").split("/")[1:])

    # -------------------------------------------------------------
    # Prepare archive path
    # -------------------------------------------------------------
    today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    archive_key = f"{extension}/{today}/{filename}"

    # -------------------------------------------------------------
    # Copy file into archive bucket
    # -------------------------------------------------------------
    s3.copy_object(
        Bucket=ARCHIVE_BUCKET,
        CopySource={"Bucket": original_bucket, "Key": original_key},
        Key=archive_key
    )

    # -------------------------------------------------------------
    # Return useful structured metadata
    # -------------------------------------------------------------
    return {
        "status": "success",
        "archive_path": f"s3://{ARCHIVE_BUCKET}/{archive_key}",
        "file_extension": extension,
        "original_path": file_s3_path
    }
