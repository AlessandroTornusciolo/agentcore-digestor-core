"""
Registering tools (Lambda functions) with AgentCore.
"""

from agentcore_manager.utils.aws_clients import agentcore_client
from agentcore_manager.utils.logging import get_logger
from agentcore_manager.utils.exceptions import ToolRegistrationError
from agentcore_manager.utils.models import ToolSpec

logger = get_logger(__name__)


def register_tool(runtime_id: str, tool: ToolSpec) -> str:
    """
    Register a Lambda-based tool in AgentCore Runtime.
    Returns tool ID.
    """
    client = agentcore_client()

    logger.info(f"Registering tool: {tool.name}")

    try:
        response = client.create_tool(
            runtimeId=runtime_id,
            name=tool.name,
            description=tool.description,
            lambdaArn=tool.lambda_arn,
            inputSchema=tool.input_schema,
        )

        tool_id = response["toolId"]
        logger.info(f"Tool registered: {tool_id}")
        return tool_id

    except Exception as e:
        logger.error(f"Tool registration failed: {e}")
        raise ToolRegistrationError(str(e))
