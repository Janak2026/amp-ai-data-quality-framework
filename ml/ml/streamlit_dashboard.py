import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
import matplotlib.pyplot as plt

# --- Load Data from SQLite ---
def load_summary_table():
    conn = sqlite3.connect("/Workspace/Repos/jana21.amazon@gmail.com/amp-ai-data-quality-framework/app_data.db")
    df = pd.read_sql_query("SELECT * FROM ai_summary", conn)
    conn.close()
    return df

def load_usage_table():
    conn = sqlite3.connect("/Workspace/Repos/jana21.amazon@gmail.com/amp-ai-data-quality-framework/app_data.db")
    df = pd.read_sql_query("SELECT * FROM usage_log", conn)
    conn.close()
    return df

# --- Streamlit UI ---
st.set_page_config(page_title="AI-DE Dashboard", layout="wide")

st.title("📊 AI-DE Mastery — Pro Dashboard")

tab1, tab2 = st.tabs(["📌 AI Summary Table", "📈 Usage Logs"])

# --- TAB 1 ---
with tab1:
    df = load_summary_table()

    st.subheader("AI Summary Table")
    st.dataframe(df, width="stretch")

# --- TAB 2 ---
with tab2:
    usage = load_usage_table()
    st.subheader("Usage Log")
    st.dataframe(usage, width="stretch")

    # Plot usage per day
    usage['date'] = pd.to_datetime(usage['timestamp']).dt.date
    counts = usage.groupby('date').size()

    fig, ax = plt.subplots(figsize=(10,5))
    counts.plot(kind='bar', ax=ax)
    ax.set_title("Daily App Usage")
    ax.set_ylabel("Number of events")
    ax.set_xlabel("Date")

    st.pyplot(fig)

st.caption("AI-DE Mastery Dashboard — Powered by Streamlit")
