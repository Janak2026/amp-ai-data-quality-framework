# metadata_summarizer.py

def summarize_metadata(
    table_name: str,
    row_count: int,
    file_size_mb: float,
    schema: dict,
    null_stats: dict = None
) -> str:
    """
    Lightweight metadata summarizer (offline, no Spark actions).
    Accepts metadata and returns a human-friendly summary.
    """

    schema_lines = [f"- {col}: {dtype}" for col, dtype in schema.items()]

    null_lines = []
    if null_stats:
        for col, pct in null_stats.items():
            null_lines.append(f"- {col}: {pct}% nulls")
    else:
        null_lines.append("No null statistics available.")

    summary = []
    summary.append(f"# Table Overview: {table_name}\n")

    summary.append("## Basic Stats")
    summary.append(f"- Row count: {row_count}")
    summary.append(f"- File size: {file_size_mb} MB\n")

    summary.append("## Schema Overview")
    summary.extend(schema_lines)
    summary.append("")

    summary.append("## Null Value Summary")
    summary.extend(null_lines)
    summary.append("")

    # Basic heuristics
    summary.append("## Data Health Observations")
    if row_count == 0:
        summary.append("- Table is empty. Needs immediate inspection.")
    elif file_size_mb == 0:
        summary.append("- File size is zero but row_count > 0 (possible corruption).")
    else:
        summary.append("- Schema and metadata look valid.")

    if null_stats:
        high_nulls = [c for c, pct in null_stats.items() if pct > 50]
        if high_nulls:
            summary.append(f"- High null percentages in: {', '.join(high_nulls)}")

    return "\n".join(summary)


if __name__ == "__main__":
    schema_example = {
        "customer_id": "string",
        "amount": "double",
        "txn_date": "date"
    }

    nulls_example = {"amount": 2.1, "txn_date": 0.0}

    output = summarize_metadata(
        table_name="transactions",
        row_count=120000,
        file_size_mb=85.3,
        schema=schema_example,
        null_stats=nulls_example
    )

    print(output)
