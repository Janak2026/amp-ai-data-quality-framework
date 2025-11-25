# Pipeline Design

## 1. Ingestion Pipeline
- PySpark reads raw CSV
- Cleans & transforms
- Writes Bronze → Silver → Gold layers
- Validated using built-in Spark functions

## 2. Profiling Pipeline
- Computes:
  - Row counts
  - Null counts per column
  - Outlier examples
  - Schema mismatch
  - Business rule violations
- Writes structured JSON summaries

## 3. LLM Summaries (Notebook 2)
- Reads llm_inputs
- Generates:
  - anomaly_summary
  - data_health
  - root_causes
- Ensures:
  - No hallucination
  - No invented examples
  - Only 4–7 sentence summaries

## 4. Post-Processing Pipeline (Notebook 3)
- Cleans LLM outputs
- Normalizes text
- Stores into llm_summaries

## 5. ML Pipeline (Week 5)
- Loads gold_customer_sales
- Trains simple prediction model
- Logs:
  - Parameters
  - Metrics
  - Artifacts
  - Signature
- Registers model

## 6. Dashboard Pipeline (Week 6)
- Local Streamlit UI
- Reads Delta tables via Databricks SQL Connector
- Shows:
  - Quality log
  - Usage log
  - Summary plots
