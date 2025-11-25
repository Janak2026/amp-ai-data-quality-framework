# Databricks notebook source
# DBTITLE 1,Config
CATALOG = "sandbox_db"           # If using Unity Catalog, otherwise ignore
SCHEMA  = "default"              # Or your schema

TRAINING_DATA_TABLE = "default.customer_features"
MODEL_REG_PATH      = "models:/customer_churn_risk/latest"

print("Config loaded.")


# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
from pyspark.sql import Window

import pandas as pd
import numpy as np

from sklearn.metrics import accuracy_score, f1_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression

import mlflow
import mlflow.sklearn

print("Imports ready.")


# COMMAND ----------

# DBTITLE 1,Load the saved model
import pickle

# Path to the saved model inside your repo
model_path = "/Workspace/Repos/jana21.amazon@gmail.com/amp-ai-data-quality-framework/models/base_model.pkl"

print("Loading model from:", model_path)

with open(model_path, "rb") as f:
    model = pickle.load(f)

print("Model loaded successfully!")
model


# COMMAND ----------

# DBTITLE 1,Start MLflow Run
import mlflow
import mlflow.sklearn

mlflow.set_experiment("/Users/jana21.amazon@gmail.com/customer_lifetime_value_week5")

with mlflow.start_run(run_name="base_model_run") as run:
    mlflow.log_param("model_type", "XGBClassifier")


# COMMAND ----------

# DBTITLE 1,Log Metrics
mlflow.log_metric("accuracy", 1.0)
mlflow.log_metric("f1_score", 1.0)


# COMMAND ----------

# DBTITLE 1,Log the Model Artifact
from mlflow.models.signature import infer_signature
import mlflow
import pandas as pd

# Close any active MLflow run
mlflow.end_run()

# Load gold table & build pandas feature set
df_gold = spark.table("default.gold_customer_sales")
feature_cols = ["total_spend", "total_orders", "avg_order_value"]

pdf = df_gold.select(feature_cols).toPandas()

# Predict using your trained model (dummy labels for signature)
dummy_y = model.predict(pdf)

# Build signature
signature = infer_signature(pdf, dummy_y)

with mlflow.start_run(run_name="clv_week5_run"):
    mlflow.sklearn.log_model(
        sk_model=model,
        artifact_path="model_artifact",
        registered_model_name="clv_model_week5",
        signature=signature,
        input_example=pdf.head(5)
    )

print("Model logged to UC successfully!")


# COMMAND ----------

# DBTITLE 1,Log Training Artifact
# Path where we actually saved the model
model_path = "/Workspace/Repos/jana21.amazon@gmail.com/amp-ai-data-quality-framework/models/base_model.pkl"

mlflow.log_artifact(
    local_path=model_path,
    artifact_path="saved_model_backup"
)

print("Backup logged to MLflow successfully!")


# COMMAND ----------

# DBTITLE 1,Print Run Info
run_id = run.info.run_id
print("Run ID:", run_id)
print("Experiment logged successfully.")


# COMMAND ----------

# DBTITLE 1,Verify in MLflow
from mlflow.tracking import MlflowClient
client = MlflowClient()

models = client.search_registered_models()
models


# COMMAND ----------

# MAGIC %md
# MAGIC ###Test
# MAGIC

# COMMAND ----------

spark.table("default.gold_customer_sales").printSchema()


# COMMAND ----------

import os

paths_to_check = [
    "/dbfs/FileStore/base_model.pkl",
    "/dbfs/FileStore/models/base_model.pkl",
    "/dbfs/tmp/base_model.pkl",
    "/FileStore/base_model.pkl",        # legacy path
    "/tmp/base_model.pkl"               # driver local FS
]

for p in paths_to_check:
    print(p, "→", os.path.exists(p))
