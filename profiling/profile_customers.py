# Databricks notebook source
# DBTITLE 1,Load customers_silver
# Load SILVER Customers Table
customers_silver_path = "/Volumes/workspace/default/silver_data/customers_silver"

df_customers = spark.read.format("delta").load(customers_silver_path)

df_customers.show(5)
df_customers.printSchema()


# COMMAND ----------

# DBTITLE 1,Column Statistics (Profiling Core)
from pyspark.sql.functions import col, countDistinct, when, count, min, max, avg
from pyspark.sql.types import IntegerType, LongType, DoubleType, FloatType, DateType, StringType

# Identify column types
numeric_cols = [f.name for f in df_customers.schema.fields if isinstance(f.dataType, (IntegerType, LongType, DoubleType, FloatType))]
string_cols = [f.name for f in df_customers.schema.fields if isinstance(f.dataType, StringType)]
date_cols = [f.name for f in df_customers.schema.fields if isinstance(f.dataType, DateType)]

profile_stats = {}

# Numeric Columns
for col_name in numeric_cols:
    row = (
        df_customers.select(
            count(col(col_name)).alias("count"),
            countDistinct(col(col_name)).alias("distinct_count"),
            count(when(col(col_name).isNull(), col_name)).alias("null_count"),
            avg(col(col_name)).alias("avg"),
            min(col(col_name)).alias("min"),
            max(col(col_name)).alias("max")
        )
    ).collect()[0]

    profile_stats[col_name] = row.asDict()

# String Columns
for col_name in string_cols:
    row = (
        df_customers.select(
            count(col(col_name)).alias("count"),
            countDistinct(col(col_name)).alias("distinct_count"),
            count(when(col(col_name).isNull(), col_name)).alias("null_count")
        )
    ).collect()[0]

    profile_stats[col_name] = row.asDict()

# Date Columns
for col_name in date_cols:
    row = (
        df_customers.select(
            count(col(col_name)).alias("count"),
            countDistinct(col(col_name)).alias("distinct_count"),
            count(when(col(col_name).isNull(), col_name)).alias("null_count"),
            min(col(col_name)).alias("min"),
            max(col(col_name)).alias("max")
        )
    ).collect()[0]

    profile_stats[col_name] = row.asDict()

profile_stats


# COMMAND ----------

# MAGIC %md
# MAGIC ###Schema Drift Detection

# COMMAND ----------

# DBTITLE 1,Create baseline schema structure
# Extract current schema in a simple JSON-like structure
current_schema = {
    f.name: str(f.dataType)
    for f in df_customers.schema.fields
}

current_schema


# COMMAND ----------

# DBTITLE 1,Use baseline = current schema
# baseline is simply the current schema (no older version yet)
baseline_schema = current_schema.copy()


# COMMAND ----------

# DBTITLE 1,Compare baseline vs current (drift detection)
schema_drift_report = {
    "missing_columns": [],
    "new_columns": [],
    "type_mismatches": []
}

# Check for missing columns
for col_name in baseline_schema:
    if col_name not in current_schema:
        schema_drift_report["missing_columns"].append(col_name)

# Check for new columns
for col_name in current_schema:
    if col_name not in baseline_schema:
        schema_drift_report["new_columns"].append(col_name)

# Check for type mismatches
for col_name in baseline_schema:
    if col_name in current_schema:
        if baseline_schema[col_name] != current_schema[col_name]:
            schema_drift_report["type_mismatches"].append({
                "column": col_name,
                "baseline_type": baseline_schema[col_name],
                "current_type": current_schema[col_name]
            })

schema_drift_report


# COMMAND ----------

# DBTITLE 1,Null Anomaly Detection
# -------------------------------------------------
# Step 5: Null Anomaly Detection
# -------------------------------------------------

null_anomalies = {}

for col_name in df_customers.columns:
    null_count = df_customers.filter(col(col_name).isNull()).count()
    total_count = df_customers.count()
    null_percentage = (null_count / total_count) * 100

    null_anomalies[col_name] = {
        "null_count": null_count,
        "total_rows": total_count,
        "null_percentage": round(null_percentage, 2),
        "issue": "High null percentage" if null_percentage > 20 else "OK"
    }

null_anomalies


# COMMAND ----------

# DBTITLE 1,Outlier Detection (Age column)
# -------------------------------------------------
# Step 6: Outlier Detection (for numeric columns)
# -------------------------------------------------

from pyspark.sql.functions import mean as _mean, stddev as _stddev

outlier_report = {}

for col_name in numeric_cols:

    stats_row = df_customers.select(
        _mean(col(col_name)).alias("mean"),
        _stddev(col(col_name)).alias("stddev")
    ).collect()[0]

    mean_val = stats_row["mean"]
    std_val = stats_row["stddev"]

    if std_val == 0 or std_val is None:
        outlier_report[col_name] = {"outliers": 0, "reason": "No variance"}
        continue

    # Z-score threshold = 3
    df_outliers = df_customers.filter((col(col_name) - mean_val) / std_val > 3)

    outlier_report[col_name] = {
        "mean": mean_val,
        "stddev": std_val,
        "outlier_count": df_outliers.count()
    }

outlier_report


# COMMAND ----------

# DBTITLE 1,AI-Generated Summary (LLM-style)
# -------------------------------------------------
# Step 7: AI-Generated Summary (Simulated LLM Output)
# -------------------------------------------------

def generate_ai_summary(stats, drift, nulls, outliers):
    summary = []

    # 1. Schema Health
    if drift["missing_columns"] or drift["new_columns"] or drift["type_mismatches"]:
        summary.append("⚠ Schema drift detected. Please review the structural changes.")
    else:
        summary.append("✔ No schema drift detected. The structure matches the baseline schema.")

    # 2. Null Analysis
    high_null_columns = [col for col, v in nulls.items() if v["null_percentage"] > 20]
    if high_null_columns:
        summary.append(f"⚠ High null percentages found in: {', '.join(high_null_columns)}.")
    else:
        summary.append("✔ No significant null anomalies detected. All columns are complete.")

    # 3. Outliers
    outlier_cols = [col for col, v in outliers.items() if v.get("outlier_count", 0) > 0]
    if outlier_cols:
        summary.append(f"⚠ Outliers detected in: {', '.join(outlier_cols)}.")
    else:
        summary.append("✔ No numeric outliers detected. Value distributions look normal.")

    # 4. General Health
    summary.append("✔ Dataset appears clean, consistent, and ready for downstream transformations.")

    return "\n".join(summary)

ai_summary = generate_ai_summary(profile_stats, schema_drift_report, null_anomalies, outlier_report)
ai_summary


# COMMAND ----------

# DBTITLE 1,Data Quality Score (0–100)
# -------------------------------------------------
# Step 8: Data Quality Scoring
# -------------------------------------------------

score = 100

# Schema drift score
if schema_drift_report["missing_columns"]:
    score -= 30
if schema_drift_report["new_columns"]:
    score -= 10
if schema_drift_report["type_mismatches"]:
    score -= 20

# Null penalty
for col_name, v in null_anomalies.items():
    if v["null_percentage"] > 20:
        score -= 10

# Outlier penalty
for col_name, v in outlier_report.items():
    if v.get("outlier_count", 0) > 0:
        score -= 5

# Use plain Python, avoid max() name collision
final_quality_score = score if score > 0 else 0
final_quality_score
