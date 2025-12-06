"""
Simple test for agent invocation.
"""

from agentcore_manager.core.invoke import invoke


def main():
    runtime_id = "<RUNTIME_ID>"
    agent_id = "<AGENT_ID>"

    result = invoke(runtime_id, agent_id, "Analyze s3://.../file.csv")
    print(result)


if __name__ == "__main__":
    main()
