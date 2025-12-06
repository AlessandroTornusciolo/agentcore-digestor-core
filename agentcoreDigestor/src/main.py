import json
from bedrock_agentcore.runtime import BedrockAgentCoreApp
from strands import Agent

from model.load import load_model

# Import tools
from tools.analyze_schema import analyze_schema
from tools.validate_data import validate_data      # ← ora lo abilitiamo veramente
from tools.schema_normalizer import schema_normalizer
# from tools.ctas import ctas
# from tools.load_into_iceberg import load_into_iceberg


app = BedrockAgentCoreApp()
log = app.logger


@app.entrypoint
async def invoke(payload, context):
    """
    AgentCore entrypoint (async generator).
    - Extracts the prompt
    - Runs the Strands agent pipeline
    - Streams output back to AgentCore
    """

    # ---- Extract user message ----
    user_message = payload.get("prompt") or payload.get("input")

    if not user_message:
        yield json.dumps({
            "status": "failed",
            "error": "No prompt provided."
        })
        return  # important for async generators

    # ---- Load LLM model ----
    model = load_model()

    # ---- Define Agent ----
    agent = Agent(
        model=model,
        system_prompt=(
            "You are the AgentCore Digestor Agent.\n"
            "Your job is to orchestrate analysis, validation, cleaning, "
            "normalization, and ingestion of data files into Iceberg tables.\n\n"
            "TOOLS AVAILABLE:\n"
            "- analyze_schema(file_s3_path)\n"
            "- validate_data(file_s3_path, schema, mode)\n"
            "- schema_normalizer(file_s3_path, mode)\n"
            "\n"
            "GUIDELINES:\n"
            "- Always use tools when appropriate.\n"
            "- Do NOT invent tools or arguments.\n"
            "- If the user requests cleaning/normalization/validation, call the appropriate tool.\n"
            "- Ask for confirmation if multiple transformation strategies are possible.\n"
            "- Output reasoning clearly and in structured form.\n"
        ),
        tools=[
            analyze_schema,
            validate_data,
            schema_normalizer,
            # load_into_iceberg,  # to be re-enabled soon
            # ctas
        ]
    )

    # ---- Stream LLM reasoning ----
    stream = agent.stream_async(user_message)

    async for event in stream:
        # LLM text output
        if isinstance(event.get("data"), str):
            yield event["data"]

        # Later: structured results
        # if "result" in event:
        #     yield json.dumps(event["result"])
