# nl_to_json.py
import re
import uuid
from pathlib import Path
from datetime import datetime

PROMPT_PATH = Path(__file__).parent / "prompts" / "nl_to_json_prompt.txt"

def simple_nl_to_json(nl_rule: str) -> dict:
    """Minimal natural language to rule JSON converter (clean + small)."""
    rule_id = "rule_" + uuid.uuid4().hex[:6]
    description = nl_rule

    # Extract possible numeric thresholds
    num_match = re.search(r"(?:>|greater than|above)\s*([\d,]+)", nl_rule, re.I)
    max_threshold = None
    if num_match:
        max_threshold = float(num_match.group(1).replace(",", ""))

    # Extract columns
    columns = []
    col_matches = re.findall(r"\b([a-z_]+_id|amount|price|quantity|date)\b", nl_rule, re.I)
    for c in col_matches:
        columns.append(c.lower())

    # If rule talks about missing
    rule_type = "business_rule"
    if "missing" in nl_rule.lower() or "null" in nl_rule.lower():
        rule_type = "null_check"

    return {
        "rule_id": rule_id,
        "description": description,
        "column": ",".join(columns),
        "rule_type": rule_type,
        "thresholds": {
            "min": None,
            "max": max_threshold,
            "regex": None
        },
        "severity": "high" if max_threshold else "medium",
        "action": "flag",
        "metadata": {
            "created_at": datetime.utcnow().date().isoformat(),
            "generated_by": "janak"
        }
    }

if __name__ == "__main__":
    print(simple_nl_to_json("Flag transactions where amount > 500000 and merchant_id is missing"))
