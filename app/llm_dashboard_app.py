import streamlit as st
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession

# ---------------------------------------------------------
# INIT
# ---------------------------------------------------------
spark = SparkSession.builder.getOrCreate()

st.set_page_config(
    page_title="AI Data Quality Dashboard",
    layout="wide"
)

st.title("📊 AI-Generated Data Quality Dashboard")
st.caption("Week 5 — Powered by your CLV model + LLM summaries")


# ---------------------------------------------------------
# LOAD DATA
# ---------------------------------------------------------
@st.cache_data
def load_summaries():
    df = spark.table("default.llm_summaries").toPandas()
    return df

df = load_summaries()

st.success(f"Loaded {len(df)} rows from `default.llm_summaries`")
st.write("### Preview")
st.dataframe(df, use_container_width=True)


# ---------------------------------------------------------
# FILTER PANEL
# ---------------------------------------------------------
st.sidebar.header("🔍 Filters")

tables = df["table_name"].unique().tolist()
selected_table = st.sidebar.selectbox("Select Table", tables)

runs = df[df["table_name"] == selected_table]["run_id"].unique().tolist()
selected_run = st.sidebar.selectbox("Select Run ID", runs)

filtered = df[
    (df["table_name"] == selected_table) &
    (df["run_id"] == selected_run)
]


# ---------------------------------------------------------
# DISPLAY RESULTS
# ---------------------------------------------------------
st.write("## 🧠 LLM Insights")

row = filtered.iloc[0]

# ---- 1. Anomaly Summary ----
st.subheader("🔹 Anomaly Summary")
st.write(row["anomaly_summary"])

# ---- 2. Data Health ----
st.subheader("🔹 Data Health Summary")
st.write(row["data_health"])

# ---- 3. Root Causes ----
st.subheader("🔹 Root Cause Analysis")
st.write(row["root_causes"])

# ---- 4. Metadata ----
st.write("---")
st.write("### Metadata")
st.json({
    "run_id": row["run_id"],
    "table": row["table_name"],
    "model_used": row["llm_model"],
    "generated_at": row["generated_at"],
    "prompt_hash": row["prompt_hash"]
})

st.info("Dashboard loaded successfully ✨")
