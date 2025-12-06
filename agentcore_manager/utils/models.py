"""
Dataclasses and typed models representing AgentCore entities.
"""

from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class ToolSpec:
    name: str
    lambda_arn: str
    description: str
    input_schema: Dict[str, Any]
