# pyspark_generator.py

import re

def generate_pyspark(nl_instruction: str) -> str:
    """
    Minimal NL → PySpark generator.
    No AI dependencies. No LLM calls.
    Lightweight keyword-based transformation.
    """

    inst = nl_instruction.lower()
    code = ""

    # --- FILTER LOGIC ---
    if "filter" in inst or "where" in inst:
        # amount > number
        m = re.search(r"(amount|price|quantity)\s*>\s*(\d+)", inst)
        if m:
            col, val = m.group(1), m.group(2)
            code = f'df.filter(df["{col}"] > {val})'

        # generic null filter
        if "not null" in inst:
            col = re.findall(r"[a-z_]+", inst)[-1]
            code = f'df.filter(df["{col}"].isNotNull())'

        # missing value filter
        if "missing" in inst or "null" in inst:
            col = re.findall(r"[a-z_]+", inst)[-1]
            code = f'df.filter(df["{col}"].isNull())'

    # --- GROUP BY / AGGREGATION ---
    elif "aggregate" in inst or "group" in inst:
        # match something like "aggregate by customer_id"
        m = re.search(r"by\s+([a-z_]+)", inst)
        group_col = m.group(1) if m else "column"

        # look for sum(amount)
        if "sum" in inst or "total" in inst:
            agg_col = "amount"
            code = (
                f'df.groupBy("{group_col}")'
                f'.agg(F.sum("{agg_col}").alias("total_{agg_col}"))'
            )

    # --- SELECT LOGIC ---
    elif "select" in inst:
        cols = re.findall(r"[a-z_]+", inst)
        cols = [c for c in cols if c not in ["select", "columns"]]
        if cols:
            code = f'df.select({", ".join([f"\\"{c}\\"" for c in cols])})'

    # --- DEFAULT FALLBACK ---
    if not code:
        code = "# Unable to generate PySpark snippet from instruction."

    return code


if __name__ == "__main__":
    print(generate_pyspark("filter transactions where amount > 50000"))
    print(generate_pyspark("aggregate by customer_id and compute total amount"))
    print(generate_pyspark("select customer_id, amount"))
