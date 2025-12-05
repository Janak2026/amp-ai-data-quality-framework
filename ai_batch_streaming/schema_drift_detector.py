# schema_drift_detector.py

def detect_schema_drift(old_schema: dict, new_schema: dict) -> dict:
    """
    Minimal schema drift detector.
    Compares two schema dictionaries: {column: datatype}
    Returns added, removed, and changed columns.
    """

    old_cols = set(old_schema.keys())
    new_cols = set(new_schema.keys())

    added = list(new_cols - old_cols)
    removed = list(old_cols - new_cols)

    changed = []
    for col in old_cols & new_cols:
        if old_schema[col] != new_schema[col]:
            changed.append({
                "column": col,
                "old_type": old_schema[col],
                "new_type": new_schema[col]
            })

    summary = []
    summary.append("# Schema Drift Summary\n")

    if added:
        summary.append("## Added Columns:")
        for col in added:
            summary.append(f"- {col}: {new_schema[col]}")
        summary.append("")

    if removed:
        summary.append("## Removed Columns:")
        for col in removed:
            summary.append(f"- {col}: previously {old_schema[col]}")
        summary.append("")

    if changed:
        summary.append("## Columns With Changed Datatypes:")
        for item in changed:
            summary.append(f"- {item['column']}: {item['old_type']} → {item['new_type']}")
        summary.append("")

    if not (added or removed or changed):
        summary.append("No schema drift detected.")

    return {
        "added": added,
        "removed": removed,
        "changed": changed,
        "summary_text": "\n".join(summary)
    }


if __name__ == "__main__":
    old_schema = {
        "customer_id": "string",
        "amount": "double",
        "txn_date": "date"
    }

    new_schema = {
        "customer_id": "string",
        "amount": "float",        # changed
        "txn_date": "date",
        "merchant_id": "string"   # added
    }

    result = detect_schema_drift(old_schema, new_schema)
    print(result["summary_text"])
