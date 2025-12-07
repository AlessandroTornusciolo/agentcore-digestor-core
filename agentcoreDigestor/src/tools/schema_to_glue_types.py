from strands import tool

@tool
def schema_to_glue_types(schema: list) -> list:
    """
    Convert Pandas dtype schema into valid Glue/Hive types.

    Input schema example:
    [
        {"name": "id", "type": "int64"},
        {"name": "name", "type": "object"},
        {"name": "amount", "type": "float64"},
        {"name": "date", "type": "object"}
    ]

    Output example:
    [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "date", "type": "string"}
    ]
    """

    glue_map = {
        "int64": "int",
        "int32": "int",
        "float64": "double",
        "float32": "double",
        "object": "string",
        "string": "string",
        "bool": "boolean",
        "datetime64[ns]": "timestamp"
    }

    converted = []

    for col in schema:
        name = col["name"]
        dtype = col["type"].lower()

        glue_type = glue_map.get(dtype, "string")  # fallback

        converted.append({
            "name": name,
            "type": glue_type
        })

    return converted
