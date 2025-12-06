"""
Create and manage an AgentCore Gateway.
"""

from agentcore_manager.utils.aws_clients import agentcore_client
from agentcore_manager.utils.logging import get_logger
from agentcore_manager.utils.exceptions import GatewayError

logger = get_logger(__name__)


def create_gateway(agent_id: str, name: str) -> str:
    """
    Create a public or private gateway for the agent.
    """
    client = agentcore_client()

    try:
        logger.info(f"Creating gateway: {name}")

        resp = client.create_gateway(
            agentId=agent_id,
            name=name,
        )

        gateway_id = resp["gatewayId"]
        logger.info(f"Gateway created: {gateway_id}")
        return gateway_id

    except Exception as e:
        logger.error(f"Gateway creation failed: {e}")
        raise GatewayError(str(e))
