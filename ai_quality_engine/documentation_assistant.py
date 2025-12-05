# documentation_assistant.py
from datetime import datetime
from pathlib import Path
import json

PROMPT_PATH = Path(__file__).parent / "prompts" / "documentation_prompt.txt"

def generate_documentation(table_name: str, schema: dict, rules: list, anomalies: list) -> str:
    """
    Minimal offline version — no LLM required.
    It generates clean human-readable documentation using simple heuristics.
    """

    # Build schema section
    schema_lines = [f"- {col}: {dtype}" for col, dtype in schema.items()]

    # Build rules section
    rule_lines = []
    for r in rules:
        rule_lines.append(f"- {r.get('description', 'Unknown rule')} (severity: {r.get('severity')})")

    # Build anomalies section
    anomaly_lines = []
    for a in anomalies:
        anomaly_lines.append(f"- {a.get('type')}: {a.get('explanation')} (severity: {a.get('severity')})")

    doc = []
    doc.append(f"# Documentation for Table: {table_name}\n")
    doc.append(f"Generated at: {datetime.utcnow().isoformat()}\n")

    # 1. Purpose (simple heuristic)
    doc.append("## Purpose")
    doc.append(f"This table stores transactional or analytical data for `{table_name}`, used across ingestion, profiling, and ML workflows.\n")

    # 2. Schema
    doc.append("## Schema Overview")
    doc.extend(schema_lines)
    doc.append("")

    # 3. Rules
    doc.append("## Data Quality Rules")
    if rule_lines:
        doc.extend(rule_lines)
    else:
        doc.append("- No rules defined.")
    doc.append("")

    # 4. Anomalies
    doc.append("## Anomaly Summary")
    if anomaly_lines:
        doc.extend(anomaly_lines)
    else:
        doc.append("- No anomalies detected.")
    doc.append("")

    # Join all lines
    return "\n".join(doc)


if __name__ == "__main__":
    # EXAMPLE RUN
    schema_example = {"amount": "double", "merchant_id": "string", "txn_date": "date"}

    rules_example = [
        {
            "description": "Flag transactions where amount > 500000 and merchant_id is missing",
            "severity": "high"
        }
    ]

    anomalies_example = [
        {
            "type": "missing_values",
            "explanation": "merchant_id contains nulls",
            "severity": "high"
        }
    ]

    output = generate_documentation("transactions", schema_example, rules_example, anomalies_example)
    print(output)
