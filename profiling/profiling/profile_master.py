# Databricks notebook source
# MAGIC %md
# MAGIC this notebook generates quality_log

# COMMAND ----------

# DBTITLE 1,Reusable rule engine
from pyspark.sql import functions as F, types as T
from datetime import datetime
import json

def validate_business_rules(table_name, df):
    issues = {}

    # ------------------------------
    # RULES FOR CUSTOMERS TABLE
    # ------------------------------
    if table_name == "customers_silver":
        issues = {
            "age_valid": df.filter((F.col("age") < 18) | (F.col("age") > 90)).count(),
            "email_valid": df.filter(~F.col("email").contains("@")).count(),
            "phone_valid": df.filter(F.length("phone") != 10).count(),
            "signup_date_valid": df.filter(F.col("signup_date") > F.current_date()).count()
        }

    # ------------------------------
    # RULES FOR SALES TABLE
    # ------------------------------
    elif table_name == "sales_silver":
        issues = {
            "quantity_positive": df.filter(F.col("quantity") <= 0).count(),
            "amount_positive": df.filter(F.col("amount") <= 0).count(),
            "product_id_not_null": df.filter(F.col("product_id").isNull()).count(),
            "sale_date_valid": df.filter(F.col("sale_date") > F.current_date()).count()
        }

    # ------------------------------
    # RULES FOR TRANSACTIONS TABLE
    # ------------------------------
    elif table_name == "transactions_silver":
        issues = {
            "amount_positive": df.filter(F.col("amount") <= 0).count(),
            "status_valid": df.filter(~F.col("status").isin(["SUCCESS", "FAILED"])).count(),
            "payment_method_valid": df.filter(~F.col("payment_method").isin(["UPI", "Card", "NetBanking", "Wallet"])).count(),
            "transaction_date_valid": df.filter(F.col("transaction_date") > F.current_date()).count()
        }

    return issues


# COMMAND ----------

# DBTITLE 1,Master Profiling Driver (profile_master)
# -----------------------------
# Config: table -> delta path
# -----------------------------
TABLES = {
    "customers_silver": "/Volumes/workspace/default/silver_data/customers_silver",
    "sales_silver": "/Volumes/workspace/default/silver_data/sales_silver",
    "transactions_silver": "/Volumes/workspace/default/silver_data/transactions_silver",
}

# Where to store the quality log (Delta table)
QUALITY_LOG_PATH = "/Volumes/workspace/default/gold_data/quality_log"


# --------------------------------------
# Helper: run profiling for one table
# --------------------------------------
def profile_table(table_name: str, path: str) -> dict:
    print(f"Running profiling for: {table_name} at {path}")
    df = spark.read.format("delta").load(path)

    # --- Column type detection ---
    numeric_cols = [f.name for f in df.schema.fields
                    if isinstance(f.dataType, (T.IntegerType, T.LongType, T.DoubleType, T.FloatType))]
    string_cols = [f.name for f in df.schema.fields
                   if isinstance(f.dataType, T.StringType)]
    date_cols = [f.name for f in df.schema.fields
                 if isinstance(f.dataType, T.DateType)]

    # --- Schema snapshot ---
    current_schema = {f.name: str(f.dataType) for f in df.schema.fields}
    baseline_schema = current_schema.copy()  # first run: baseline = current

    schema_drift_report = {
        "missing_columns": [],
        "new_columns": [],
        "type_mismatches": []
    }

    # (No real drift yet, but framework is ready)
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

    # --- Null anomalies ---
    null_anomalies = {}
    total_rows = df.count()

    for col_name in df.columns:
        null_count = df.filter(F.col(col_name).isNull()).count()
        null_pct = (null_count / total_rows) * 100 if total_rows > 0 else 0

        null_anomalies[col_name] = {
            "null_count": null_count,
            "total_rows": total_rows,
            "null_percentage": round(null_pct, 2),
            "issue": "High null percentage" if null_pct > 20 else "OK"
        }

    # --- Outliers for numeric columns ---
    outlier_report = {}

    for col_name in numeric_cols:
        stats_row = df.select(
            F.mean(F.col(col_name)).alias("mean"),
            F.stddev(F.col(col_name)).alias("stddev")
        ).collect()[0]

        mean_val = stats_row["mean"]
        std_val = stats_row["stddev"]

        if std_val is None or std_val == 0:
            outlier_report[col_name] = {"outlier_count": 0, "reason": "No variance"}
            continue

        df_outliers = df.filter((F.col(col_name) - mean_val) / std_val > 3)

        outlier_report[col_name] = {
            "mean": float(mean_val) if mean_val is not None else None,
            "stddev": float(std_val),
            "outlier_count": df_outliers.count()
        }

    # --- AI-style summary (simulated) ---
    def generate_ai_summary(table_name, drift, nulls, outliers):
        summary = []

        # Schema
        if drift["missing_columns"] or drift["new_columns"] or drift["type_mismatches"]:
            summary.append(f"⚠ Schema drift detected in {table_name}. Please review structural changes.")
        else:
            summary.append(f"✔ No schema drift detected in {table_name}. Schema matches baseline.")

        # Nulls
        high_null_cols = [c for c, v in nulls.items() if v["null_percentage"] > 20]
        if high_null_cols:
            summary.append(f"⚠ High null percentages found in: {', '.join(high_null_cols)}.")
        else:
            summary.append(f"✔ No significant null anomalies detected in {table_name}.")

        # Outliers
        outlier_cols = [c for c, v in outliers.items() if v.get("outlier_count", 0) > 0]
        if outlier_cols:
            summary.append(f"⚠ Outliers detected in numeric columns: {', '.join(outlier_cols)}.")
        else:
            summary.append(f"✔ No numeric outliers detected in {table_name}.")

        summary.append(f"✔ Overall, {table_name} looks healthy and ready for downstream use.")
        return "\n".join(summary)

    ai_summary = generate_ai_summary(table_name, schema_drift_report, null_anomalies, outlier_report)

    # --- Quality score ---
    score = 100

    if schema_drift_report["missing_columns"]:
        score -= 30
    if schema_drift_report["new_columns"]:
        score -= 10
    if schema_drift_report["type_mismatches"]:
        score -= 20

    for _, v in null_anomalies.items():
        if v["null_percentage"] > 20:
            score -= 10

    for _, v in outlier_report.items():
        if v.get("outlier_count", 0) > 0:
            score -= 5

    final_quality_score = score if score > 0 else 0

    # --- Build result dict (to be stored in Delta) ---
    result = {
        "table_name": table_name,
        "run_time_utc": datetime.utcnow().isoformat(),
        "quality_score": int(final_quality_score),
        "ai_summary": ai_summary,
        "schema_drift_json": json.dumps(schema_drift_report),
        "null_anomalies_json": json.dumps(null_anomalies),
        "outlier_report_json": json.dumps(outlier_report),
    }

    return result


# --------------------------------------
# Run profiling for all configured tables
# --------------------------------------
results = []

for tbl, path in TABLES.items():
    print(f"Processing table: {tbl}")

    # Load DataFrame for business rule checks
    df = spark.read.format("delta").load(path)

    # Run profiling (stats, drift, nulls, outliers, AI summary, score)
    res = profile_table(tbl, path)

    # Run business rules for this table
    business_issues = validate_business_rules(tbl, df)

    # Add business rule results into the result dictionary
    res["business_rules_json"] = json.dumps(business_issues)

    # Add final object to the list of results
    results.append(res)

results

# COMMAND ----------

# df = spark.read.format("delta").load("/Volumes/workspace/default/gold_data/quality_log")
# df.printSchema()


# COMMAND ----------

# %sql
# ALTER TABLE delta.`/Volumes/workspace/default/gold_data/quality_log`
# ADD COLUMNS (business_rules_json STRING);


# COMMAND ----------

# DBTITLE 1,Save results into Delta table
# Convert Python list of dicts -> Spark DataFrame
quality_df = spark.createDataFrame(results)

quality_df.show(truncate=False)
quality_df.printSchema()

# Write/append to Delta table
(
    quality_df
        .write
        .format("delta")
        .mode("append")   # append so you can run this daily in future
        .save(QUALITY_LOG_PATH)
)

print("Quality log updated at:", QUALITY_LOG_PATH)


# COMMAND ----------

# DBTITLE 1,Validate delta table
df_quality_log = spark.read.format("delta").load(QUALITY_LOG_PATH)
df_quality_log.show(truncate=False)
# df_quality_log.select("table_name", "business_rules_json").show(truncate=False)


# COMMAND ----------

# DBTITLE 1,LLM Integration Steps
# Run LLM Integration Steps
%run ../ai/notebook_01_prepare_inputs
%run ../ai/notebook_02_call_llm
%run ../ai/notebook_03_postprocess_and_store

