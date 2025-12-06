# AgentCore Python SDK Development Guidelines
(v1.0 — valid for AWS AgentCore Runtime 2025)

## 1. Python Version
* Use Python 3.12+.
* Ensure compatibility with the latest AWS SDK (boto3 / boto3-stubs or boto3 v2 when stable).
* Always run in an isolated environment (virtualenv, venv, poetry, or pipenv).

## 2. Project Structure
Recommended layout:
```bash
agentcore_manager/
    __init__.py
    config/
        settings.py          # shared configuration loader
    core/
        runtime.py           # create/list/delete runtime
        agent.py             # create/update/delete agent profiles
        tools.py             # register lambda tools
        policies.py          # create and attach policies
        gateway.py           # create agent gateway
        invoke.py            # functions to invoke the runtime
    utils/
        aws_clients.py       # boto3 clients creation, retry wrappers
        logging.py           # logger configuration
        exceptions.py        # custom exception types
    scripts/
        setup_agentcore.py   # sequential setup orchestrator
        test_invoke.py       # test script for agent invocation
```
Goals:
* Clear separation of responsibilities
* Composable modules
* Single responsibility per file

## 3. Coding Style
### 3.1 Typing
* Use full PEP 484 typing, including `TypedDict` or `pydantic` models if needed.
* Avoid untyped dicts unless unavoidable.
* Use `list[str]` instead of `List[str]` (Python 3.9+ syntax).

### 3.2 Docstrings
* Use Google-style or reStructuredText-style docstrings.
* Keep comments essential and minimal, not narrative.

### 3.3 Naming
* Functions: `snake_case`
* Classes / dataclasses: `PascalCase`
* Constants: `UPPER_CASE`
* Modules: `lowercase`

### 3.4 Linting
* Follow PEP8 using tools such as:
    - `ruff`
    - `flake8`
    - `black` (with minimal formatting config)

## 4. AWS SDK Usage
### 4.1 Always use client dependency injection
Avoid:
```python
client = boto3.client("xyz")
```
Prefer:
```python
from agentcore_manager.utils.aws_clients import agentcore_client

client = agentcore_client()  # typed, centralized config
```

### 4.2 Use retry wrappers for AWS throttling
Define a central retry utility:
```python
@retry(stop=stop_after_attempt(5), wait=wait_exponential())
def call_aws(method, *args, **kwargs):
    return method(*args, **kwargs)
```

### 4.3 Typed AWS responses
Use `boto3-stubs` when possible:
```bash
pip install boto3-stubs[agentcore,lambda,s3,athena,sts,iam]
```

## 5. Configuration & Environment
### 5.1 Centralized config
Use a config file:
```bash
agentcore_manager/config/settings.py
```
It loads from:
* environment variables
* `.env` file
* defaults in the repository

Use `pydantic` or a minimal handwritten loader.

### 5.2 AWS credentials
Let the AWS default chain handle it:
* ENV vars
* Shared profiles
* Lambda execution role (when deployed)

Never hardcode credentials.

## 6. Dataclasses for Internal Representation
Use dataclasses for:
* Runtime description
* Agent description
* Tool registration requests
* Policy definitions
* Gateway configuration

Example:
```python
@dataclass
class ToolSpec:
    name: str
    lambda_arn: str
    description: str
    input_schema: dict[str, Any]
```
This improves maintainability.

## 7. Error Handling
### 7.1 Custom errors
Create custom exceptions:
```python
class AgentCoreRuntimeError(Exception):
    pass
```

### 7.2 Raise, don’t return error strings
Bad:
```python
return {"error": "runtime failed"}
```
Good:
```python
raise AgentCoreRuntimeError("Runtime creation failed")
```

### 7.3 Wrap AWS errors with meaningful messages

## 8. Logging
Use a unified Logger (structured logs recommended):
```python
import logging

logger = logging.getLogger("agentcore")
logger.setLevel(logging.INFO)
```
Do not litter code with prints.

## 9. Functions Granularity
Prefer:
* Small, pure functions
* No side effects unless in dedicated orchestration modules
Example:
* `create_runtime()` → returns runtime_id
* `register_tool()` → returns tool_id
* `link_tool_to_agent()` → attaches tool to agent

## 10. AgentCore-Oriented Design Principles
### 10.1 Tools must be stateless
AgentCore invokes tools many times in a session.

### 10.2 Runtime must be configured once
Treat runtime as global shared resource.

### 10.3 Agents should be composable
Enable future features:
* multi-agent reasoning
* chained agents
* supervisor agent

### 10.4 Input/output schemas must be JSON-based
AgentCore passes JSON, not strings or OpenAPI schemas.

## 11. Deliverables to Include in This Project
✔ `agentcore_runtime.py`
* create/list/delete runtime

✔ `agent.py`
* create/update agent (instructions, personality)

✔ `tools.py`
* register lambda tools
* define tool schema
* attach tools to an agent

✔ `gateway.py`
* create runtime gateway endpoint

✔ `invoke.py`
* invoke_agentcore_runtime()

### ✔ Orchestrator script:
```bash
scripts/setup_agentcore.py
```
Which:
* creates runtime
* creates agent
* registers tools
* attaches tools
* creates gateway
* validates everything

## 12. Versioning & Documentation
* Tag releases in Git (`v0.1`, `v0.2`, …).
* Include README with:
    - setup instructions
    - example invocations
    - troubleshooting