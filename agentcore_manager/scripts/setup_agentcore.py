"""
Sequential orchestrator:
Creates AgentCore Runtime using Python SDK.
"""

from agentcore_manager.core.runtime import create_runtime


def main():
    runtime_id = create_runtime(
        name="agentcore-digestor-runtime-dev",
        foundation_model="anthropic.claude-3-sonnet-20240229-v1:0"
    )

    print(f"Runtime created. ID = {runtime_id}")


if __name__ == "__main__":
    main()
