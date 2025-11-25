# Databricks notebook source
# DBTITLE 1,CONFIG
# ---- CONFIG ----
# You are using the built-in default database (no catalog, no UC)
QUALITY_LOG_TABLE  = "default.quality_log"
LLM_INPUTS_TABLE   = "default.llm_inputs"

from pyspark.sql import functions as F


# COMMAND ----------

# DBTITLE 1,Clean + Convert Structs/Arrays → Strings
# Load profiling output from your Pipeline
df = spark.read.table(QUALITY_LOG_TABLE)

# Convert maps/arrays/structs to human-readable JSON text
def to_str(col):
    return F.when(F.col(col).isNotNull(), F.to_json(F.col(col))).otherwise(F.lit("None"))

df2 = (
    df
    .withColumn("statistics_s",               to_str("statistics"))
    .withColumn("null_summary_s",             to_str("null_summary"))
    .withColumn("outlier_summary_s",          to_str("outlier_summary"))
    .withColumn("schema_drift_summary_s",     to_str("schema_drift_summary"))
    .withColumn("business_rule_violations_s", to_str("business_rule_violations"))
)


# COMMAND ----------

# DBTITLE 1,Aggregate to single row per (table_name, run_id)
inputs = (
    df2
    .groupBy("table_name", "run_id")
    .agg(
        F.max("timestamp").alias("timestamp"),
        F.first("statistics_s").alias("statistics"),
        F.first("null_summary_s").alias("null_summary"),
        F.first("outlier_summary_s").alias("outlier_summary"),
        F.first("schema_drift_summary_s").alias("schema_drift_summary"),
        F.first("business_rule_violations_s").alias("business_rule_violations")
    )
)


# COMMAND ----------

# DBTITLE 1,Create payload JSON (not used by LLM yet but useful to inspect)
inputs = inputs.withColumn(
    "payload_json",
    F.to_json(
        F.struct(
            "table_name",
            "run_id",
            "statistics",
            "null_summary",
            "outlier_summary",
            "schema_drift_summary",
            "business_rule_violations",
            "timestamp"
        )
    )
)


# COMMAND ----------

# DBTITLE 1,Write to Delta table
inputs.write.format("delta").mode("overwrite").saveAsTable(LLM_INPUTS_TABLE)

display(inputs)
print("LLM Input Table Written:", LLM_INPUTS_TABLE)


# COMMAND ----------

spark.sql("SHOW DATABASES").show()

# COMMAND ----------

spark.sql("SHOW TABLES IN default").show()
