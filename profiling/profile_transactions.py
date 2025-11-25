# Databricks notebook source
# DBTITLE 1,Load teansaction_silver
from pyspark.sql import functions as F, types as T

tx_silver_path = "/Volumes/workspace/default/silver_data/transactions_silver"

df_tx = spark.read.format("delta").load(tx_silver_path)

df_tx.show(5)
df_tx.printSchema()


# COMMAND ----------

# DBTITLE 1,Column Statistics
numeric_cols = [f.name for f in df_tx.schema.fields if isinstance(f.dataType, (T.IntegerType, T.LongType, T.DoubleType, T.FloatType))]
string_cols  = [f.name for f in df_tx.schema.fields if isinstance(f.dataType, T.StringType)]
date_cols    = [f.name for f in df_tx.schema.fields if isinstance(f.dataType, T.DateType)]

profile_stats = {}

# Numeric
for col_name in numeric_cols:
    row = (
        df_tx.select(
            F.count(F.col(col_name)).alias("count"),
            F.countDistinct(F.col(col_name)).alias("distinct_count"),
            F.count(F.when(F.col(col_name).isNull(), col_name)).alias("null_count"),
            F.avg(F.col(col_name)).alias("avg"),
            F.min(F.col(col_name)).alias("min"),
            F.max(F.col(col_name)).alias("max")
        )
    ).collect()[0]
    profile_stats[col_name] = row.asDict()

# String
for col_name in string_cols:
    row = (
        df_tx.select(
            F.count(F.col(col_name)).alias("count"),
            F.countDistinct(F.col(col_name)).alias("distinct_count"),
            F.count(F.when(F.col(col_name).isNull(), col_name)).alias("null_count")
        )
    ).collect()[0]
    profile_stats[col_name] = row.asDict()

# Date
for col_name in date_cols:
    row = (
        df_tx.select(
            F.count(F.col(col_name)).alias("count"),
            F.countDistinct(F.col(col_name)).alias("distinct_count"),
            F.count(F.when(F.col(col_name).isNull(), col_name)).alias("null_count"),
            F.min(F.col(col_name)).alias("min"),
            F.max(F.col(col_name)).alias("max")
        )
    ).collect()[0]
    profile_stats[col_name] = row.asDict()

profile_stats


# COMMAND ----------

# DBTITLE 1,Schema Drift Detection
current_schema = {f.name: str(f.dataType) for f in df_tx.schema.fields}
baseline_schema = current_schema.copy()

schema_drift_report = {
    "missing_columns": [],
    "new_columns": [],
    "type_mismatches": []
}

for col_name in baseline_schema:
    if col_name not in current_schema:
        schema_drift_report["missing_columns"].append(col_name)

for col_name in current_schema:
    if col_name not in baseline_schema:
        schema_drift_report["new_columns"].append(col_name)

for col_name in baseline_schema:
    if col_name in current_schema and baseline_schema[col_name] != current_schema[col_name]:
        schema_drift_report["type_mismatches"].append({
            "column": col_name,
            "baseline_type": baseline_schema[col_name],
            "current_type": current_schema[col_name]
        })

schema_drift_report


# COMMAND ----------

# DBTITLE 1,Null Anomaly Detection
null_anomalies = {}
total_rows = df_tx.count()

for col_name in df_tx.columns:
    null_count = df_tx.filter(F.col(col_name).isNull()).count()
    null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0

    null_anomalies[col_name] = {
        "null_count": null_count,
        "total_rows": total_rows,
        "null_percentage": round(null_percentage, 2),
        "issue": "High null percentage" if null_percentage > 20 else "OK"
    }

null_anomalies


# COMMAND ----------

# DBTITLE 1,Outlier detection (amount only)
outlier_report = {}

for col_name in numeric_cols:
    stats_row = df_tx.select(
        F.mean(F.col(col_name)).alias("mean"),
        F.stddev(F.col(col_name)).alias("stddev")
    ).collect()[0]

    mean_val = stats_row["mean"]
    std_val = stats_row["stddev"]

    if std_val == 0 or std_val is None:
        outlier_report[col_name] = {"outlier_count": 0, "reason": "No variance"}
        continue

    df_outliers = df_tx.filter((F.col(col_name) - mean_val) / std_val > 3)

    outlier_report[col_name] = {
        "mean": mean_val,
        "stddev": std_val,
        "outlier_count": df_outliers.count()
    }

outlier_report


# COMMAND ----------

# DBTITLE 1,AI-style summary
def generate_ai_summary_transactions(stats, drift, nulls, outliers):
    summary = []

    if drift["missing_columns"] or drift["new_columns"] or drift["type_mismatches"]:
        summary.append("⚠ Schema drift detected in transactions_silver. Please review the structural changes.")
    else:
        summary.append("✔ No schema drift detected in transactions_silver. Schema matches the baseline.")

    high_null_columns = [col for col, v in nulls.items() if v["null_percentage"] > 20]
    if high_null_columns:
        summary.append(f"⚠ High null percentages found in: {', '.join(high_null_columns)}.")
    else:
        summary.append("✔ No significant null anomalies detected in transactions_silver.")

    outlier_cols = [col for col, v in outliers.items() if v.get("outlier_count", 0) > 0]
    if outlier_cols:
        summary.append(f"⚠ Outliers detected in numeric columns: {', '.join(outlier_cols)}.")
    else:
        summary.append("✔ No numeric outliers detected in transactions_silver.")

    summary.append("✔ Dataset appears consistent and ready for downstream fraud analysis or payment behavior modelling.")
    return "\n".join(summary)

ai_summary = generate_ai_summary_transactions(profile_stats, schema_drift_report, null_anomalies, outlier_report)
ai_summary


# COMMAND ----------

# DBTITLE 1,Data Quality Score (0–100)
score = 100

if schema_drift_report["missing_columns"]:
    score -= 30
if schema_drift_report["new_columns"]:
    score -= 10
if schema_drift_report["type_mismatches"]:
    score -= 20

for col_name, v in null_anomalies.items():
    if v["null_percentage"] > 20:
        score -= 10

for col_name, v in outlier_report.items():
    if v.get("outlier_count", 0) > 0:
        score -= 5

final_quality_score = score if score > 0 else 0
final_quality_score
