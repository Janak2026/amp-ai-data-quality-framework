# streaming_explainer.py

def explain_streaming_pipeline(name: str, source: str, sink: str, frequency: str, checkpoints: str = None) -> str:
    """
    Minimal helper that explains a streaming pipeline in clean English.
    No Spark execution. No dependencies.
    """

    explanation = []
    explanation.append(f"# Streaming Pipeline: {name}\n")

    explanation.append("## Overview")
    explanation.append(
        f"This streaming pipeline continuously reads data from **{source}**, "
        f"processes it in small incremental batches, and writes the results to **{sink}**.\n"
    )

    explanation.append("## Processing Frequency")
    explanation.append(f"- Trigger interval: {frequency}\n")

    if checkpoints:
        explanation.append("## Checkpointing")
        explanation.append(
            f"- The pipeline uses checkpointing at **{checkpoints}** to store offsets and maintain exactly-once or at-least-once guarantees.\n"
        )

    explanation.append("## What the Pipeline Typically Does")
    explanation.append("- Reads new data as soon as it arrives.")
    explanation.append("- Applies basic cleaning, transformations, and validations.")
    explanation.append("- Maintains state if required (aggregations or session logic).")
    explanation.append("- Writes processed results to the target sink.\n")

    explanation.append("## How AI Enhances This Pipeline")
    explanation.append("- AI can monitor schema changes in real-time.")
    explanation.append("- AI can generate alerts for sudden spikes or drops in event volume.")
    explanation.append("- AI can classify anomalies in near real-time.")
    explanation.append("- AI can auto-generate validation rules for incoming data.\n")

    explanation.appen
