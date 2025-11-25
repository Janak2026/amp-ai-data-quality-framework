# Databricks notebook source
# DBTITLE 1,Imports
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, f1_score
from sklearn.ensemble import RandomForestClassifier

from pyspark.sql import functions as F


# COMMAND ----------

# DBTITLE 1,Load GOLD Customers dataset
df = spark.table("default.gold_customer_sales")
display(df)


# COMMAND ----------

# DBTITLE 1,Create Label Column
df_labeled = df.withColumn(
    "is_high_value",
    F.when(
        (F.col("total_spend") >= 5000) | (F.col("avg_order_value") >= 200),
        1
    ).otherwise(0)
)

display(df_labeled)


# COMMAND ----------

# DBTITLE 1,Select Features
feature_cols = ["total_spend", "total_orders", "avg_order_value"]

ml_df = df_labeled.select(feature_cols + ["is_high_value"])
display(ml_df)


# COMMAND ----------

# DBTITLE 1,Convert to Pandas
pdf = ml_df.toPandas().dropna()

X = pdf[feature_cols]
y = pdf["is_high_value"]


# COMMAND ----------

# DBTITLE 1,Train/Test Split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)


# COMMAND ----------

# DBTITLE 1,Train Random Forest
model = RandomForestClassifier(
    n_estimators=100,
    max_depth=8,
    random_state=42
)

model.fit(X_train, y_train)


# COMMAND ----------

# DBTITLE 1,Evaluate
preds = model.predict(X_test)

acc = accuracy_score(y_test, preds)
f1 = f1_score(y_test, preds)

print("Accuracy:", acc)
print("F1 Score:", f1)


# COMMAND ----------

# DBTITLE 1,Save Model Locally (Before MLflow)
import pickle
import os

# Save inside your repo — SAFE location
repo_path = "/Workspace/Repos/jana21.amazon@gmail.com/amp-ai-data-quality-framework/models"
os.makedirs(repo_path, exist_ok=True)

model_path = repo_path + "/base_model.pkl"

print("Saving model to:", model_path)

with open(model_path, "wb") as f:
    pickle.dump(model, f)

print("Model saved successfully:", model_path)


# COMMAND ----------

# MAGIC %md
# MAGIC ###Test

# COMMAND ----------

df = spark.table("default.gold_customer_sales")
df.printSchema()


# COMMAND ----------

spark.sql("SHOW TABLES IN default").show(1000)


# COMMAND ----------

spark.sql("SHOW DATABASES").show(1000)


# COMMAND ----------

dbutils.fs.ls("/Workspace/Repos/jana21.amazon@gmail.com/amp-ai-data-quality-framework/gold/")


# COMMAND ----------

spark.sql("SHOW TABLES IN default").show(1000)


# COMMAND ----------

spark.sql("SHOW DATABASES").show()


# COMMAND ----------

dbutils.fs.ls("/Workspace/Repos/jana21.amazon@gmail.com/amp-ai-data-quality-framework/silver/")


# COMMAND ----------

dbutils.fs.ls("/Workspace/Repos/jana21.amazon@gmail.com/amp-ai-data-quality-framework/silver/")
