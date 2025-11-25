# Databricks notebook source
# DBTITLE 1,Create a fake quality_log table
from pyspark.sql import Row
from pyspark.sql import functions as F

# Drop if exists
spark.sql("DROP TABLE IF EXISTS sandbox_db.quality_log")

sample_data = [
    Row(
        table_name="customers",
        run_id="run_001",
        timestamp="2025-01-10 12:00:00",

        statistics={
            "row_count": 1000,
            "min_age": 18,
            "max_age": 75
        },

        null_summary={
            "email": 50,
            "phone": 120
        },

        outlier_summary={
            "age": {"count": 5, "examples": [120, 3]}
        },

        schema_drift_summary={
            "missing_columns": ["middle_name"],
            "extra_columns": ["tmp_debug_col"]
        },

        business_rule_violations={
            "age_negative": 2,
            "invalid_email_format": 34
        }
    )
]

df_fake = spark.createDataFrame(sample_data)
df_fake.write.format("delta").mode("overwrite").saveAsTable("sandbox_db.quality_log")

display(df_fake)
