"""
utils.py  — Common utilities for Week 4 LLM Integration
"""

import os
import json
import hashlib
import re
from typing import Dict


# ---------------------------------------------------
# 1. Load prompt template from file
# ---------------------------------------------------
def load_template(path: str) -> str:
    """
    Reads a prompt template (.txt file) and returns the content.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Prompt file not found: {path}")
    with open(path, "r") as f:
        return f.read()


# ---------------------------------------------------
# 2. Render prompt with {{placeholders}}
# ---------------------------------------------------
def render_prompt(template: str, context: Dict[str, str]) -> str:
    """
    Replace placeholders in the template with actual values.
    Example: {{table_name}} → "customers"
    """
    output = template
    for key, value in context.items():
        placeholder = "{{" + key + "}}"
        output = output.replace(placeholder, str(value) if value is not None else "")
    return output


# ---------------------------------------------------
# 3. Convert Spark maps/arrays/structs to JSON-like text
# ---------------------------------------------------
def safe_string(value):
    """
    Converts Spark Row/array/map into pretty JSON text for LLM safety.
    Guarantees the LLM receives clean, readable text.
    """
    try:
        if value is None:
            return "None"
        if isinstance(value, (dict, list)):
            return json.dumps(value, indent=2)
        return str(value)
    except:
        return str(value)


# ---------------------------------------------------
# 4. Sanitize final LLM output before storing in Delta
# ---------------------------------------------------
def sanitize_text(text: str, max_len: int = 4000) -> str:
    """
    Ensures LLM output is clean and safe to store.
    Removes extra whitespace and limits length.
    """
    if text is None:
        return ""
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > max_len:
        return text[:max_len - 3] + "..."
    return text


# ---------------------------------------------------
# 5. Prompt hash for version tracking
# ---------------------------------------------------
def prompt_hash(template_text: str) -> str:
    """
    Produces a short fingerprint of the prompt template.
    Used to track which template version generated the output.
    """
    return hashlib.sha1(template_text.encode("utf-8")).hexdigest()[:10]


# ---------------------------------------------------
# 6. Placeholder LLM call
# ---------------------------------------------------
def call_llm(prompt: str):
    """
    Placeholder function for LLM call.
    Replace with:
      - Databricks Foundation Model endpoint
      - LangChain OpenAI client
      - Azure OpenAI
    """
    raise Exception(
        "call_llm() not implemented. Replace with actual LLM client (OpenAI, Databricks FM, etc.)."
    )
