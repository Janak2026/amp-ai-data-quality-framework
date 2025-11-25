# Databricks notebook source
# DBTITLE 1,Install dependencies
# CELL 1 — Install dependencies (run once in the environment)
# Run in a notebook cell or terminal where Streamlit will execute.
%pip install streamlit pandas matplotlib plotly seaborn


# COMMAND ----------

# DBTITLE 1,Imports & Config
# CELL 2 — Imports & config
import os
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
from datetime import datetime

# Databricks / repo paths & table names
LLM_SUMMARIES_TABLE = "default.llm_summaries"
GOLD_CUSTOMER_TABLE = "default.gold_customer_sales"
LLM_INPUTS_TABLE = "default.llm_inputs"

# Use uploaded image as logo (developer-provided path)
LOGO_PATH = "/mnt/data/77359e94-daf2-488b-9a19-e7d4b3b868e1.png"

# UI constants
PAGE_TITLE = "AI-DE Data Quality Monitor"
PAGE_ICON = "📊"
st.set_page_config(page_title=PAGE_TITLE, page_icon=PAGE_ICON, layout="wide")


# COMMAND ----------

# DBTITLE 1,Helper: load data (Databricks-aware)
# CELL 3 — Data loading helpers
# In Databricks notebooks, `spark` is available. If running locally, replace with reading from CSV.
def load_llm_summaries():
    try:
        df = spark.table(LLM_SUMMARIES_TABLE).toPandas()
    except Exception as e:
        st.error(f"Could not load {LLM_SUMMARIES_TABLE}: {e}")
        # fallback: empty df
        df = pd.DataFrame(columns=[
            "table_name","run_id","anomaly_summary","data_health",
            "root_causes","prompt_hash","llm_model","generated_at"
        ])
    return df

def load_gold_customers():
    try:
        df = spark.table(GOLD_CUSTOMER_TABLE).toPandas()
    except Exception as e:
        st.error(f"Could not load {GOLD_CUSTOMER_TABLE}: {e}")
        df = pd.DataFrame(columns=["customer_id","total_spend","total_orders","avg_order_value","first_purchase_date","last_purchase_date"])
    return df

def load_llm_inputs():
    try:
        df = spark.table(LLM_INPUTS_TABLE).toPandas()
    except Exception:
        df = pd.DataFrame()
    return df


# COMMAND ----------

# DBTITLE 1,Metrics computation (KPI calculations)
# CELL 4 — Metrics computation
def compute_kpis(gold_df, summaries_df):
    kpis = {}
    # Basic customer KPIs
    if not gold_df.empty:
        kpis["customers"] = int(gold_df.shape[0])
        kpis["avg_order_value"] = float(gold_df["avg_order_value"].mean())
        kpis["total_revenue"] = float(gold_df["total_spend"].sum())
    else:
        kpis["customers"] = 0
        kpis["avg_order_value"] = 0.0
        kpis["total_revenue"] = 0.0

    # Data quality KPIs from summaries
    if not summaries_df.empty:
        # simple heuristics to compute rates
        # count tables flagged with moderate/poor health
        summaries_df["health_clean"] = summaries_df["data_health"].fillna("").str.lower()
        kpis["poor_health_tables"] = int(summaries_df[summaries_df["health_clean"].str.contains("poor")].shape[0])
        kpis["moderate_health_tables"] = int(summaries_df[summaries_df["health_clean"].str.contains("moderate")].shape[0])
        # extract simple null/outlier stats from anomaly_summary if present (best-effort)
        # Not a perfect numeric extractor — used for quick KPI only
        summaries_df["has_nulls"] = summaries_df["anomaly_summary"].fillna("").str.contains("null", case=False)
        kpis["tables_with_nulls"] = int(summaries_df["has_nulls"].sum())
    else:
        kpis["poor_health_tables"] = 0
        kpis["moderate_health_tables"] = 0
        kpis["tables_with_nulls"] = 0

    return kpis


# COMMAND ----------

# DBTITLE 1,UI: Header + Sidebar filters
# CELL 5 — UI Header & Sidebar
def render_header():
    col1, col2 = st.columns([1, 9])
    with col1:
        # show logo if available
        if os.path.exists(LOGO_PATH):
            st.image(LOGO_PATH, width=110)
        else:
            st.write("")  # placeholder
    with col2:
        st.title(PAGE_TITLE)
        st.markdown("**AI-enabled Data Quality Dashboard** — LLM summaries, KPIs, and drill-downs")

# Sidebar filters
def render_sidebar(summaries_df):
    st.sidebar.header("Filters")
    run_ids = summaries_df["run_id"].unique().tolist() if not summaries_df.empty else []
    selected_run = st.sidebar.selectbox("Select run_id", options=["ALL"] + run_ids, index=0)
    table_names = summaries_df["table_name"].unique().tolist() if not summaries_df.empty else []
    selected_table = st.sidebar.selectbox("Table filter", options=["ALL"] + table_names, index=0)
    min_revenue = st.sidebar.number_input("Min total_spend (filter customers)", value=0, step=100)
    return selected_run, selected_table, min_revenue


# COMMAND ----------

# DBTITLE 1,UI: KPI cards & charts
# CELL 6 — KPI cards & charts
def render_kpis_and_charts(kpis, gold_df, summaries_df):
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Customers", kpis["customers"])
    k2.metric("Total Revenue", f"₹{kpis['total_revenue']:,.0f}")
    k3.metric("Avg Order Value", f"₹{kpis['avg_order_value']:,.2f}")
    k4.metric("Tables w/ Nulls", kpis["tables_with_nulls"])

    st.markdown("### Data Health Overview")
    # health donut / pie
    health_counts = {
        "Poor": kpis["poor_health_tables"],
        "Moderate": kpis["moderate_health_tables"],
        "Good": max(0, summaries_df.shape[0] - (kpis["poor_health_tables"] + kpis["moderate_health_tables"]))
    }
    fig = px.pie(names=list(health_counts.keys()), values=list(health_counts.values()), title="Table Health Distribution")
    st.plotly_chart(fig, use_container_width=True)

    # Trend of revenue / orders
    if not gold_df.empty:
        st.markdown("### Revenue vs Orders (Top customers)")
        top_customers = gold_df.sort_values("total_spend", ascending=False).head(10)
        fig2 = px.bar(top_customers, x="customer_id", y="total_spend", hover_data=["total_orders","avg_order_value"], title="Top 10 Customers by Revenue")
        st.plotly_chart(fig2, use_container_width=True)


# COMMAND ----------

# DBTITLE 1,UI: LLM Summaries Explorer (expandable, copyable)
# CELL 7 — LLM Summaries explorer
def render_summaries_explorer(summaries_df, selected_run, selected_table):
    st.markdown("## LLM Summaries (Anomalies, Health & Root Causes)")

    if summaries_df.empty:
        st.info("No LLM summaries found.")
        return

    # filters
    df = summaries_df.copy()
    if selected_run and selected_run != "ALL":
        df = df[df["run_id"] == selected_run]
    if selected_table and selected_table != "ALL":
        df = df[df["table_name"] == selected_table]

    for idx, row in df.iterrows():
        with st.expander(f"{row['table_name']} — {row['run_id']} — {row['llm_model']}"):
            st.subheader("Anomaly Summary")
            st.write(row["anomaly_summary"])
            st.subheader("Data Health")
            st.write(row["data_health"])
            st.subheader("Root Causes")
            st.markdown(row["root_causes"])
            cols = st.columns([1,1,1])
            with cols[0]:
                st.download_button(
                    label="Download Summary",
                    data=f"anomaly_summary:\n{row['anomaly_summary']}\n\nhealth:\n{row['data_health']}\n\nroot_causes:\n{row['root_causes']}",
                    file_name=f"{row['table_name']}_{row['run_id']}_summary.txt"
                )
            with cols[1]:
                st.button("Mark as Reviewed", key=f"review_{idx}")
            with cols[2]:
                st.code(f"prompt_hash: {row.get('prompt_hash','')}\nmodel: {row.get('llm_model','')}")


# COMMAND ----------

# DBTITLE 1,UI: Customer drill-down & export
# CELL 8 — Customer drill-down & export
def render_customer_drilldown(gold_df, min_revenue):
    st.markdown("## Customer Drill-Down")
    if gold_df.empty:
        st.info("No customer data available.")
        return

    filtered = gold_df[gold_df["total_spend"] >= min_revenue].copy()
    # quick derived columns
    filtered["last_purchase_date"] = pd.to_datetime(filtered["last_purchase_date"])
    filtered["first_purchase_date"] = pd.to_datetime(filtered["first_purchase_date"])
    filtered["days_since_last_purchase"] = (pd.Timestamp.now() - filtered["last_purchase_date"]).dt.days

    st.dataframe(filtered.sort_values("total_spend", ascending=False).reset_index(drop=True))
    csv = filtered.to_csv(index=False)
    st.download_button("Export filtered customers CSV", data=csv, file_name="filtered_customers.csv")


# COMMAND ----------

# DBTITLE 1,UI: Footer & run
# CELL 9 — App runner
def main():
    render_header()

    summaries_df = load_llm_summaries()
    gold_df = load_gold_customers()
    llm_inputs = load_llm_inputs()

    selected_run, selected_table, min_revenue = render_sidebar(summaries_df)
    kpis = compute_kpis(gold_df, summaries_df)

    render_kpis_and_charts(kpis, gold_df, summaries_df)
    render_summaries_explorer(summaries_df, selected_run, selected_table)
    render_customer_drilldown(gold_df, min_revenue)

    st.markdown("---")
    st.caption(f"Last updated: {datetime.utcnow().isoformat()} UTC")

if __name__ == "__main__":
    main()
