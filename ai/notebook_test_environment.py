# Databricks notebook source
# DBTITLE 1,Create the default database (safe even if it exists)
spark.sql("CREATE DATABASE IF NOT EXISTS default")


# COMMAND ----------

# DBTITLE 1,Test
spark.sql("SHOW TABLES IN default").show()


# COMMAND ----------

# DBTITLE 1,Create your test table: default.quality_log
from pyspark.sql.types import *
from pyspark.sql import Row

# Define schema manually
schema = StructType([
    StructField("table_name", StringType(), True),
    StructField("run_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    
    StructField("statistics", 
        StructType([
            StructField("row_count", IntegerType(), True),
            StructField("min_age", IntegerType(), True),
            StructField("max_age", IntegerType(), True)
        ]), 
    True),
    
    StructField("null_summary",
        StructType([
            StructField("email", IntegerType(), True),
            StructField("phone", IntegerType(), True)
        ]),
    True),
    
    StructField("outlier_summary",
        StructType([
            StructField("age", 
                StructType([
                    StructField("count", IntegerType(), True),
                    StructField("examples", ArrayType(IntegerType()), True)
                ])
            )
        ]),
    True),
    
    StructField("schema_drift_summary",
        StructType([
            StructField("missing_columns", ArrayType(StringType()), True),
            StructField("extra_columns", ArrayType(StringType()), True)
        ]),
    True),
    
    StructField("business_rule_violations",
        StructType([
            StructField("age_negative", IntegerType(), True),
            StructField("invalid_email_format", IntegerType(), True)
        ]),
    True)
])

# Create data using the schema
data = [
    (
        "customers",
        "run_001",
        "2025-01-10 12:00:00",
        {"row_count": 1000, "min_age": 18, "max_age": 75},
        {"email": 50, "phone": 120},
        {"age": {"count": 5, "examples": [120, 3]}},
        {"missing_columns": ["middle_name"], "extra_columns": ["tmp_debug_col"]},
        {"age_negative": 2, "invalid_email_format": 34}
    )
]

df_fake = spark.createDataFrame(data, schema=schema)

df_fake.write.format("delta").mode("overwrite").saveAsTable("default.quality_log")

display(df_fake)


# COMMAND ----------

spark.sql("SHOW TABLES IN default").show()
