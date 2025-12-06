"""
Sequential orchestrator: creates runtime, agent, tools and gateway.
"""

from agentcore_manager.core.runtime import create_runtime
from agentcore_manager.core.agent import create_agent
from agentcore_manager.core.tools import register_tool
from agentcore_manager.utils.models import ToolSpec


def main():
    runtime_id = create_runtime(
        name="agentcore-runtime-dev",
        foundation_model="anthropic.claude-3-sonnet-20240229-v1:0"
    )

    agent_id = create_agent(
        runtime_id=runtime_id,
        name="IngestionAgent",
        instruction="You are an ingestion orchestrator..."
    )

    # Example tool registration
    tool = ToolSpec(
        name="analyze_file_schema",
        lambda_arn="<FILL_ME>",
        description="Analyze S3 file schema",
        input_schema={"type": "object"}
    )

    register_tool(runtime_id, tool)

    print("Setup complete.")


if __name__ == "__main__":
    main()
