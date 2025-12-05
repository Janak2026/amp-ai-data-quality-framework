# json_to_pyspark.py

def rule_to_pyspark(rule: dict) -> str:
    """Very small and simple conversion: JSON → PySpark condition."""
    col = rule.get("column", "")
    columns = [c.strip() for c in col.split(",") if c.strip()]

    conditions = []

    # Handle null / missing rules
    if rule["rule_type"] == "null_check":
        for c in columns:
            conditions.append(f"df.filter(df['{c}'].isNull())")

    # Handle threshold rules
    max_val = rule["thresholds"].get("max")
    if max_val is not None and columns:
        conditions.append(f"df.filter(df['{columns[0]}'] > {max_val})")

    if not conditions:
        return "# No validation condition generated."

    return "\n".join([f"# Validation Rule: {rule['description']}", *[c + ".count()" for c in conditions]])
