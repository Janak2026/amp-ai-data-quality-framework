
# ✅ **README.md (Final Version — Copy/Paste into GitHub)**

````markdown
# 🚀 AMP–AI Data Quality Framework  
**Automated Data Quality + LLM Anomaly Summaries + MLflow Tracking + Streamlit Dashboard**

A production-ready, end-to-end **AI-powered Data Quality Framework** built using **PySpark, Delta Lake, MLflow, and LLM automation**.  
This project ingests raw data, performs validation & anomaly checks, generates AI-based summaries, tracks ML models, and exposes all insights via a **Streamlit dashboard**.

Designed for real-world Data Engineering & MLOps workflows.

---

## 🌟 Key Features

### 🔹 1. PySpark Ingestion Pipeline
- Batch ingestion of raw CSV/JSON files  
- Bronze → Silver → Gold Delta Lake architecture  
- Schema evolution handling  
- Automated metadata logging  

### 🔹 2. Data Profiling & Anomaly Detection  
- Null checks  
- Outlier detection  
- Schema drift comparison  
- Business rule validation  

All stored in **Delta Lake** with historical tracking.

### 🔹 3. LLM-Powered Data Quality Summaries  
Uses **LangChain + local LLMs (Phi-3 Mini)** to generate:
- Anomaly summaries  
- Data health reports  
- Root-cause analysis  

Completely automated based on profiling outputs.

### 🔹 4. ML Model Training + Tracking  
- CLV prediction model built using scikit-learn  
- MLflow experiment + run tracking  
- Model signature + lineage  
- Registered into Unity Catalog (optional)

### 🔹 5. Streamlit Dashboard (Local + Databricks Compatible)
Visualizes:
- Data quality summaries  
- Usage logs  
- Generated AI reports  

Runs locally with:

```bash
streamlit run dashboard_app.py
```

---

## 🏗️ Architecture Overview

```
                ┌────────────────────────────────────────┐
                │          Raw Data Sources               │
                └────────────────────────────────────────┘
                               │
                               ▼
                   ┌────────────────────┐
                   │  PySpark Ingestion │
                   └────────────────────┘
                               │
                               ▼
                ┌──────────────────────────────────┐
                │  Profiling + Validation Layer     │
                │ (nulls, outliers, schema drift)   │
                └──────────────────────────────────┘
                               │
                               ▼
           ┌───────────────────────────────────────────────┐
           │        LLM Summary Generation (Phi-3)         │
           │  anomaly_summary / data_health / root_causes  │
           └───────────────────────────────────────────────┘
                               │
                               ▼
                ┌──────────────────────────────────┐
                │     ML Model + MLflow Tracking    │
                └──────────────────────────────────┘
                               │
                               ▼
                ┌──────────────────────────────────┐
                │      Streamlit Dashboard UI       │
                └──────────────────────────────────┘
```

---

## 📂 Repository Structure

```
amp-ai-data-quality-framework/
│
├── ai/                 # LLM prompts, LangChain logic
├── app/                # Streamlit dashboard
├── docs/               # Documentation (architecture, design, profiling)
├── ingestion/          # PySpark ingestion pipeline
├── ml/                 # MLflow + model training scripts
├── models/             # Saved pickle models
├── profiling/          # Data validation & anomaly detection
└── README.md
```

---

## ⚙️ Tech Stack

| Layer | Technology |
|-------|------------|
| Storage | Delta Lake, DBFS, UC |
| Compute | PySpark, Databricks CE |
| AI / LLM | LangChain, Phi-3 Mini, Gemma (optional) |
| ML | scikit-learn, MLflow |
| Dashboard | Streamlit |
| Tools | GitHub, DBFS, pandas, numpy |

---

## ▶️ Running the Local Dashboard

### **1. Install dependencies**
```bash
pip install streamlit pandas matplotlib
```

### **2. Run the dashboard**
```bash
streamlit run dashboard_app.py
```

### **3. Local files used**
- `quality_log.csv`
- `usage_log.csv`

---

## 📈 MLflow Model Tracking

The project supports:
- Experiment creation
- Run logging
- Model serialization
- Model signature inference
- Registered model versioning

ML artifacts are stored under:

```
models/base_model.pkl
```

---

## 📜 Documentation (Full Set)

All documents live in:  
`/docs/`

Includes:
- System architecture  
- Data model  
- Summary generation  
- MLflow usage  
- Profiling flow  
- Deployment guides  
- Governance  

---

## 🤝 Contributing

Contributions are welcome!  
For major changes, please open an issue before submitting a PR.

---

## 📄 License

MIT License  
You are free to use, modify, and distribute this project.

---

## 👤 Author

### **Janardhana Rao Komanapalli (Janak)**  
Senior Data Engineer — Big Data | Spark | Azure | AI Integration  
📍 India  

🔗 **LinkedIn:** https://www.linkedin.com/in/janardhanarao-dataengineer/
🔗 **GitHub:** https://github.com/Janak2026  

---

## ⭐ If you found this helpful  
Please ⭐ **star the repository** to support the project!

````END````
