# Databricks notebook source
# DBTITLE 1,Install dependencies & restart Python
# MAGIC %pip install transformers==4.41.2 accelerate sentencepiece
# MAGIC %restart_python
# MAGIC

# COMMAND ----------

# DBTITLE 1,Imports
import os
import pandas as pd
import pyspark.sql.functions as F
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM


# COMMAND ----------

# DBTITLE 1,Config
LLM_INPUTS_TABLE      = "default.llm_inputs"
LLM_RAW_OUTPUTS_TABLE = "default.llm_raw_outputs"

PROMPTS_DIR = "/Workspace/Repos/jana21.amazon@gmail.com/amp-ai-data-quality-framework/ai/prompts/"

DRY_RUN = False


# COMMAND ----------

# DBTITLE 1,Load prompt templates + utils.py
with open(PROMPTS_DIR + "anomaly_summary_prompt.txt") as f:
    anomaly_template_txt = f.read()
with open(PROMPTS_DIR + "data_health_prompt.txt") as f:
    health_template_txt = f.read()
with open(PROMPTS_DIR + "root_cause_prompt.txt") as f:
    root_template_txt = f.read()

import importlib.util, sys
utils_path = "/Workspace/Repos/jana21.amazon@gmail.com/amp-ai-data-quality-framework/ai/utils.py"
spec = importlib.util.spec_from_file_location("ai_utils", utils_path)
ai_utils = importlib.util.module_from_spec(spec)
sys.modules["ai_utils"] = ai_utils
spec.loader.exec_module(ai_utils)

print("Templates + utils loaded.")


# COMMAND ----------

# DBTITLE 1,Load Phi-3 Mini (4k Instruct) Model
model_name = "microsoft/Phi-3-mini-4k-instruct"

print("Loading:", model_name)

tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)
model = AutoModelForCausalLM.from_pretrained(model_name, trust_remote_code=True)

device = "cuda" if torch.cuda.is_available() else "cpu"
model.to(device)

model.eval()

print("Model loaded on:", device)


# COMMAND ----------

# DBTITLE 1,Load input rows
df = spark.table(LLM_INPUTS_TABLE).toPandas()
print("Loaded rows:", len(df))
df.head()


# COMMAND ----------

# DBTITLE 1,LLM Generation Loop (Validator Block + Phi-3 Mini)
def run_llm(prompt, max_new_tokens=180):
    validator = """
[ROLE]
You are an AI Data Quality Analyst.

RULES:
- Use ONLY the provided statistics/nulls/outliers/schema drift/business rule violations.
- No invented numbers.
- No hallucination.
- No chat dialogue.
- No code.
- No markdown except '-' for bullets (root causes only).
- Stop after producing the required content.
- Output must be clean and focused.

[ANSWER]
"""

    final_prompt = validator + prompt.strip()

    inputs = tokenizer(final_prompt, return_tensors="pt").to(device)

    with torch.no_grad():
        outputs = model.generate(
            **inputs,
            max_new_tokens=max_new_tokens,
            temperature=0.2,
            top_p=0.9,
            do_sample=True,
            eos_token_id=tokenizer.eos_token_id,
            pad_token_id=tokenizer.eos_token_id
        )

    text = tokenizer.decode(outputs[0], skip_special_tokens=True)
    return text.replace("[ANSWER]", "").strip()


rows = []

for _, r in df.iterrows():
    ctx = {
        "statistics": r["statistics"],
        "null_summary": r["null_summary"],
        "outlier_summary": r["outlier_summary"],
        "schema_drift_summary": r["schema_drift_summary"],
        "business_rule_violations": r["business_rule_violations"]
    }

    if DRY_RUN:
        anomaly_text = "[DRY_RUN]\n" + ai_utils.render_prompt(anomaly_template_txt, ctx)
        health_text  = "[DRY_RUN]\n" + ai_utils.render_prompt(health_template_txt, ctx)
        root_text    = "[DRY_RUN]\n" + ai_utils.render_prompt(root_template_txt, ctx)
    else:
        final_anom   = ai_utils.render_prompt(anomaly_template_txt, ctx)
        final_health = ai_utils.render_prompt(health_template_txt, ctx)
        final_root   = ai_utils.render_prompt(root_template_txt, ctx)

        anomaly_text = run_llm(final_anom)
        health_text  = run_llm(final_health)
        root_text    = run_llm(final_root)

    rows.append({
        "table_name": r["table_name"],
        "run_id": r["run_id"],
        "anomaly_summary": anomaly_text,
        "data_health": health_text,
        "root_causes": root_text,
        "prompt_hash": ai_utils.prompt_hash(anomaly_template_txt + health_template_txt + root_template_txt),
        "llm_model": model_name,
        "generated_at": r["timestamp"]
    })

print("LLM generation completed for", len(rows), "rows.")


# COMMAND ----------

# DBTITLE 1,Save to Delta
spark_df = spark.createDataFrame(pd.DataFrame(rows))

spark_df.write.format("delta").mode("overwrite").saveAsTable(LLM_RAW_OUTPUTS_TABLE)

display(spark_df)
print("Saved to:", LLM_RAW_OUTPUTS_TABLE)
