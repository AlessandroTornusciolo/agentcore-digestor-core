"""
Centralized configuration loader for AgentCore Manager.
Loads environment variables and provides strongly-typed settings.
"""

from dataclasses import dataclass
import os


@dataclass
class Settings:
    aws_region: str
    runtime_name: str

    @staticmethod
    def load() -> "Settings":
        return Settings(
            aws_region=os.getenv("AWS_REGION", "eu-central-1"),
            runtime_name=os.getenv("AGENTCORE_RUNTIME_NAME", "agentcore-runtime-dev"),
        )


settings = Settings.load()
