"""
AgentCore Runtime management.
"""

from typing import Optional
from agentcore_manager.utils.aws_clients import agentcore_client
from agentcore_manager.utils.logging import get_logger
from agentcore_manager.utils.exceptions import RuntimeCreationError

logger = get_logger(__name__)


def create_runtime(name: str, foundation_model: str) -> str:
    """
    Create an AgentCore runtime.
    Returns the runtime ID.
    """
    client = agentcore_client()

    try:
        logger.info(f"Creating AgentCore runtime: {name}")

        response = client.create_runtime(
            name=name,
            foundationModel=foundation_model,
        )

        runtime_id = response["runtimeId"]
        logger.info(f"Runtime created: {runtime_id}")
        return runtime_id

    except Exception as e:
        logger.error(f"Runtime creation failed: {e}")
        raise RuntimeCreationError(str(e))
