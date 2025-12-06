"""
Factory for AWS SDK clients used across the AgentCore Manager.
"""

import boto3
from botocore.client import BaseClient
from typing import Callable
from .logging import get_logger

logger = get_logger(__name__)


def _client(service: str) -> BaseClient:
    logger.debug(f"Creating AWS client: {service}")
    return boto3.client(service)


def agentcore_client() -> BaseClient:
    return _client("agentcore")


def lambda_client() -> BaseClient:
    return _client("lambda")


def s3_client() -> BaseClient:
    return _client("s3")


def glue_client() -> BaseClient:
    return _client("glue")
