"""
Factory for AWS SDK clients used across the AgentCore Manager.
"""

import boto3
from botocore.client import BaseClient
from agentcore_manager.utils.logging import get_logger

logger = get_logger(__name__)


def _client(service: str) -> BaseClient:
    logger.debug(f"Creating AWS client: {service}")
    return boto3.client(service)


# --- AgentCore API clients ---

def agentcore_client() -> BaseClient:
    """
    Configuration API for AgentCore.
    Used to create runtime, agent, tools, policies, gateway.
    """
    return _client("bedrock-agentcore")


def agentcore_runtime_client() -> BaseClient:
    """
    Runtime execution API for AgentCore.
    Used to invoke the agent.
    """
    return _client("bedrock-agent-runtime")


def agentcore_control_client() -> BaseClient:
    """
    Control & versioning API for AgentCore.
    Handles prepareAgent, createVersion, publish.
    """
    return _client("bedrock-agentcore-control")


# Other AWS services
def lambda_client() -> BaseClient:
    return _client("lambda")


def s3_client() -> BaseClient:
    return _client("s3")


def glue_client() -> BaseClient:
    return _client("glue")
