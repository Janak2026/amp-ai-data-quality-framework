# pipeline_assistant.py

def explain_pipeline(stages: list) -> str:
    """
    Minimal pipeline explanation helper.
    Accepts a list of pipeline stage names and returns a simple text explanation.
    """

    explanation = []
    explanation.append("# Pipeline Explanation\n")

    explanation.append("## Stages Overview")
    for idx, stage in enumerate(stages, start=1):
        explanation.append(f"{idx}. {stage}")
    explanation.append("")

    explanation.append("## What the Pipeline Does")
    if len(stages) == 0:
        explanation.append("No stages defined.")
    else:
        explanation.append("This pipeline processes data through a series of well-defined steps,")
        explanation.append("from ingestion to profiling, transformation, and preparation for downstream analytics.\n")

    explanation.append("## Stage-by-Stage Purpose")
    for stage in stages:
        lower = stage.lower()
        if "ingest" in lower:
            explanation.append(f"- **{stage}:** Reads raw data and loads it into a structured format.")
        elif "profile" in lower:
            explanation.append(f"- **{stage}:** Performs data validation, statistics, and anomaly checks.")
        elif "gold" in lower or "transform" in lower:
            explanation.append(f"- **{stage}:** Applies business transformations and produces final analytical tables.")
        elif "ml" in lower:
            explanation.append(f"- **{stage}:** Trains or evaluates ML models using prepared features.")
        else:
            explanation.append(f"- **{stage}:** General processing step in the pipeline.")
    explanation.append("")

    explanation.append("## Recommended Improvements")
    if "ingest" in " ".join(stages).lower():
        explanation.append("- Add schema validation before ingestion to catch bad files early.")
    if "profile" in " ".join(stages).lower():
        explanation.append("- Track anomaly history to detect long-term patterns.")
    if "gold" in " ".join(stages).lower():
        explanation.append("- Add data quality rules at the gold stage for business KPIs.")
    if len(stages) > 5:
        explanation.append("- Consider breaking large pipelines into modular components.")

    if len(stages) == 0:
        explanation.append("- Define at least one pipeline stage.")

    return "\n".join(explanation)


if __name__ == "__main__":
    sample_stages = [
        "ingest_customers",
        "profile_customers",
        "gold_customer_sales",
        "ml_training"
    ]

    print(explain_pipeline(sample_stages))
