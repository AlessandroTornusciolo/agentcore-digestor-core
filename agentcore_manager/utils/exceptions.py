"""
Custom exception classes for AgentCore Manager.
"""


class AgentCoreError(Exception):
    """Base class for AgentCore-related errors."""


class RuntimeCreationError(AgentCoreError):
    """Raised when runtime creation fails."""


class ToolRegistrationError(AgentCoreError):
    """Raised when registering a tool fails."""


class GatewayError(AgentCoreError):
    """Raised when creating or invoking a gateway fails."""
