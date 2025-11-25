# Project Architecture (Phase 1)

## 1. Ingestion Layer (Week 2)
- Source: synthetic e-commerce dataset
- Tools: PySpark, Delta Lake
- Tables:
  - bronze_customer_sales
  - silver_customer_sales
  - gold_customer_transactions

## 2. Profiling & Quality Layer (Week 3)
- Null checks
- Outlier detection
- Schema drift detection
- Business rule validations
- Output tables:
  - default.quality_log
  - default.llm_inputs

## 3. AI / LLM Layer (Week 4)
- Prompts built using structured instructions
- Phi-3 Mini and TinyLlama used for summarization
- Output saved to:
  - default.llm_raw_outputs
  - default.llm_summaries

## 4. ML Layer (Week 5)
- CLV Prediction Model (DecisionTreeRegressor)
- MLflow signature + model registry
- Saved at:
  - models/base_model.pkl

## 5. Dashboard Layer (Week 6)
- Streamlit dashboard (local mode)
- Reads:
  - default.quality_log
  - default.usage_log
