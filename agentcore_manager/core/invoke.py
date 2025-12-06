"""
Invoke AgentCore runtime.
"""

from agentcore_manager.utils.aws_clients import agentcore_client
from agentcore_manager.utils.logging import get_logger

logger = get_logger(__name__)


def invoke(runtime_id: str, agent_id: str, input_text: str) -> dict:
    """
    Invoke the agent with arbitrary text.
    """
    client = agentcore_client()

    logger.info(f"Invoking agent: {agent_id}")

    response = client.invoke_runtime(
        runtimeId=runtime_id,
        agentId=agent_id,
        inputText=input_text,
    )

    return response
