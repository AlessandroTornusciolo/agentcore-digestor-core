"""
Agent profile creation & update.
"""

from agentcore_manager.utils.aws_clients import agentcore_client
from agentcore_manager.utils.logging import get_logger

logger = get_logger(__name__)


def create_agent(runtime_id: str, name: str, instruction: str) -> str:
    """
    Create an agent bound to a runtime.
    Returns the agent ID.
    """
    client = agentcore_client()

    logger.info(f"Creating agent: {name}")

    response = client.create_agent(
        runtimeId=runtime_id,
        name=name,
        instruction=instruction,
    )

    agent_id = response["agentId"]
    logger.info(f"Agent created: {agent_id}")
    return agent_id
