"""
AgentCore Runtime management using AWS bedrock-agentcore API.
"""

from agentcore_manager.utils.aws_clients import agentcore_client
from agentcore_manager.utils.logging import get_logger
from agentcore_manager.utils.exceptions import RuntimeCreationError


logger = get_logger(__name__)


def create_runtime(name: str, foundation_model: str) -> str:
    """
    Create an AgentCore Runtime.

    Args:
        name: Friendly name of the runtime.
        foundation_model: Model ID (e.g. anthropic.claude-3-sonnet-20240229-v1:0)

    Returns:
        runtime_id: The ID of the created runtime.
    """

    client = agentcore_client()

    try:
        logger.info(f"Creating AgentCore Runtime: {name}")

        response = client.create_runtime(
            name=name,
            foundationModel=foundation_model
        )

        runtime_id = response["runtimeId"]

        logger.info(f"Runtime created successfully: {runtime_id}")
        return runtime_id

    except Exception as e:
        logger.error(f"Failed to create AgentCore runtime: {e}")
        raise RuntimeCreationError(str(e))
