# engine_orchestrator.py

from ai_quality_engine.nl_to_json import simple_nl_to_json
from ai_quality_engine.json_to_pyspark import rule_to_pyspark
from ai_quality_engine.anomaly_classifier import classify_anomaly
from ai_quality_engine.documentation_assistant import generate_documentation
import json

def generate_rule(nl_rule: str):
    """Convert NL rule → JSON → PySpark snippet."""
    rule_json = simple_nl_to_json(nl_rule)
    pyspark_snippet = rule_to_pyspark(rule_json)
    return rule_json, pyspark_snippet

def classify(text: str):
    """Classify an anomaly using minimal classifier."""
    return classify_anomaly(text)

def create_docs(table_name: str, schema: dict, rules: list, anomalies: list):
    """Generate minimal documentation."""
    return generate_documentation(table_name, schema, rules, anomalies)

def run_all(nl_rule: str, anomaly_text: str, schema: dict, table_name: str):
    """Run everything end-to-end in a single lightweight function."""
    
    # 1) Rule generation
    rule_json, pyspark_code = generate_rule(nl_rule)

    # 2) Anomaly classification
    anomaly_info = classify(anomaly_text)

    # 3) Documentation (use rule + anomaly)
    doc_text = create_docs(
        table_name=table_name,
        schema=schema,
        rules=[rule_json],
        anomalies=[anomaly_info]
    )

    return {
        "rule_json": rule_json,
        "pyspark_code": pyspark_code,
        "anomaly_classification": anomaly_info,
        "documentation": doc_text
    }


if __name__ == "__main__":
    # Example input
    nl_example = "Flag transactions where amount > 500000 and merchant_id is missing"
    anomaly_example = "Found missing merchant_id in transactions"
    schema_example = {
        "amount": "double",
        "merchant_id": "string",
        "txn_date": "date"
    }

    result = run_all(
        nl_rule=nl_example,
        anomaly_text=anomaly_example,
        schema=schema_example,
        table_name="transactions"
    )

    print("\n=== RULE JSON ===")
    print(json.dumps(result["rule_json"], indent=2))

    print("\n=== SPARK SNIPPET ===")
    print(result["pyspark_code"])

    print("\n=== ANOMALY CLASSIFICATION ===")
    print(json.dumps(result["anomaly_classification"], indent=2))

    print("\n=== DOCUMENTATION OUTPUT ===")
    print(result["documentation"])
