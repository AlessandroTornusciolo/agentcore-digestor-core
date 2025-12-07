import json
from bedrock_agentcore.runtime import BedrockAgentCoreApp
from strands import Agent
from model.load import load_model

# --------- TOOLS IMPORTATI (solo quelli realmente utilizzati) ----------
from tools.analyze_schema import analyze_schema
from tools.validate_data import validate_data
from tools.schema_normalizer import schema_normalizer
from tools.load_into_iceberg import load_into_iceberg
from tools.create_iceberg_table import create_iceberg_table
# schema_to_glue_types → RIMOSSO COMPLETAMENTE


app = BedrockAgentCoreApp()
log = app.logger


@app.entrypoint
async def invoke(payload, context):
    """
    AgentCore entrypoint (async generator).
    Runs Strands agent pipeline and streams output.
    """

    user_message = payload.get("prompt") or payload.get("input")

    if not user_message:
        yield json.dumps({
            "status": "failed",
            "error": "No prompt provided."
        })
        return

    # Load the foundation model
    model = load_model()

    # ---------------- SYSTEM PROMPT NUOVO E RIPULITO ----------------
    system_prompt = """
You are the AgentCore Digestor Agent.

Your role is to orchestrate HIGH-QUALITY ingestion of data into Iceberg tables
using the available tools.

IMPORTANT:
You can be asked to perform partial tasks (e.g., “just analyze this file”,
“just validate”, “just normalize”) OR full ingestion.
Choose the appropriate tools based strictly on user intent.

### INGESTION PIPELINE (MANDATORY WHEN USER REQUESTS INGESTION)

If the user explicitly asks to *ingest*, *load into Iceberg*, *create a table*,
or perform the *full pipeline*, ALWAYS follow this sequence:

1. Call `analyze_schema`
2. Call `validate_data` (when data quality or ingestion is requested)
3. Call `schema_normalizer`
4. ALWAYS call `load_into_iceberg` BEFORE creating metadata
5. ONLY AFTER writing Parquet, call `create_iceberg_table`
6. NEVER skip `load_into_iceberg`
7. NEVER call `create_iceberg_table` before data has been written
8. NEVER invent schemas. Use only the schema returned by `schema_normalizer`.
9. NEVER use Pandas dtypes like int64/object in CTAS — ALWAYS rely on the normalized schema.

### PARTIAL OPERATIONS (ALLOWED)

If the user asks for:
- “give me the schema”
- “validate this file”
- “normalize this file”
- “what does this file contain”
- any diagnostic task

→ Perform ONLY the tools required for that request.

### GENERAL RULES
- Use tools precisely and only when relevant.
- Decide minimally and avoid assumptions.
- When ingestion is requested, ALWAYS enforce the mandatory pipeline.


"""

    # Create Strands Agent
    agent = Agent(
        model=model,
        system_prompt=system_prompt,
        tools=[
            analyze_schema,
            validate_data,
            schema_normalizer,
            load_into_iceberg,
            create_iceberg_table
        ]
    )

    # Stream output
    stream = agent.stream_async(user_message)

    async for event in stream:
        if isinstance(event.get("data"), str):
            yield event["data"]
    return
