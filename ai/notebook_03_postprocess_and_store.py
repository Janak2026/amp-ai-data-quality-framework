# Databricks notebook source
spark.sql("DROP TABLE IF EXISTS default.llm_summaries")

# COMMAND ----------

# DBTITLE 1,Config
INPUT_TABLE  = "default.llm_raw_outputs"   # Notebook02 output
OUTPUT_TABLE = "default.llm_summaries"


# COMMAND ----------

# DBTITLE 1,Import
import re
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# COMMAND ----------

# DBTITLE 1,Load Data
df = spark.read.table(INPUT_TABLE)
display(df)


# COMMAND ----------

# DBTITLE 1,Clean functions
def clean_root_causes(text: str) -> str:
    if not text:
        return ""

    lines = text.split("\n")
    bullets = []

    for line in lines:
        line = line.strip()

        # Must begin as a bullet
        if not line.startswith("- "):
            continue

        # FINAL skip list — expanded for Outliers JSON leak
        if any(skip in line.lower() for skip in [
            "use only",
            "invented",
            "hallucination",
            "chat",
            "code",
            "markdown",
            "rules",
            "stop after",
            "output must",
            "statistics",
            "nulls:",
            "schema",
            "business rule",
            "bullet points are allowed",
            "generic advice",
            "repetition",
            "outliers:",     # <-- NEW IMPORTANT FILTER
            "{"              # remove JSON-like bullets
        ]):
            continue

        bullets.append(line)

    bullets = bullets[:4]
    return "\n".join(bullets)


# COMMAND ----------

# DBTITLE 1,Register UDFs
clean_paragraph_udf = F.udf(clean_paragraph, StringType())
clean_root_udf      = F.udf(clean_root_causes, StringType())


# COMMAND ----------

# DBTITLE 1,Apply Cleaning
cleaned = (
    df
    .withColumn("anomaly_summary", clean_paragraph_udf("anomaly_summary"))
    .withColumn("data_health", clean_paragraph_udf("data_health"))
    .withColumn("root_causes", clean_root_udf("root_causes"))
)

display(cleaned)


# COMMAND ----------

# DBTITLE 1,Save to Delta (Final Table)
# ---- SAVE CLEANED TABLE ----
cleaned.write.format("delta").mode("overwrite").saveAsTable(OUTPUT_TABLE)
print("LLM summaries saved →", OUTPUT_TABLE)


# ---- FINAL VALIDATION ----
print("Validating final LLM summaries table…")

final_tbl = spark.table(OUTPUT_TABLE)

display(final_tbl)   # <-- THIS IS THE CORRECT display SYNTAX

print("Row count:", final_tbl.count())
print("Table loaded successfully:", OUTPUT_TABLE)


# COMMAND ----------

# MAGIC %md
# MAGIC Your LLM pipeline is now complete:
# MAGIC
# MAGIC Notebook	                        Purpose
# MAGIC notebook_01_prepare_inputs	        Create clean inputs for LLM
# MAGIC notebook_02_call_llm	            Generate summaries via LLM
# MAGIC notebook_03_postprocess_and_store	Merge final summaries into main Delta table