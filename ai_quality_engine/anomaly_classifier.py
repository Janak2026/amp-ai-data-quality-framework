# anomaly_classifier.py
import re
from datetime import datetime

def classify_anomaly(text: str) -> dict:
    """
    Very small, heuristic-based anomaly classifier.
    No heavy logic. No ML. Works on simple keyword patterns.
    """

    anomaly_type = "unknown"
    severity = "medium"
    explanation = ""

    t = text.lower()

    # ---- schema drift ----
    if "new column" in t or "column mismatch" in t or "schema" in t:
        anomaly_type = "schema_drift"
        severity = "high"
        explanation = "A schema-level change was detected."

    # ---- missing values ----
    elif "null" in t or "missing" in t:
        anomaly_type = "missing_values"
        severity = "high"
        explanation = "Records contain missing or null values."

    # ---- outliers ----
    elif re.search(r"\b(outlier|above|below|greater than|> )\b", t):
        anomaly_type = "outlier"
        severity = "high"
        explanation = "Several values fall outside expected numeric range."

    # ---- distribution shift ----
    elif "distribution" in t or "skew" in t or "drift" in t:
        anomaly_type = "distribution_shift"
        severity = "medium"
        explanation = "Data distribution has changed compared to historical patterns."

    # ---- business rule violation ----
    elif "rule" in t or "violation" in t or "constraint" in t:
        anomaly_type = "business_rule_violation"
        severity = "high"
        explanation = "Detected a violation of predefined business logic."

    else:
        explanation = "Unable to determine anomaly type from description."

    return {
        "type": anomaly_type,
        "severity": severity,
        "explanation": explanation,
        "classified_at": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    sample = "Found missing merchant_id values in 42% of rows"
    print(classify_anomaly(sample))
