# Databricks notebook source
# DBTITLE 1,Load SILVER Tables
# Load SILVER Customers
customers_silver_path = "/Volumes/workspace/default/silver_data/customers_silver"
df_customers = spark.read.format("delta").load(customers_silver_path)

# Load SILVER Transactions
transactions_silver_path = "/Volumes/workspace/default/silver_data/transactions_silver"
df_tx = spark.read.format("delta").load(transactions_silver_path)

df_customers.show(3)
df_tx.show(3)


# COMMAND ----------

# DBTITLE 1,Join Customers + Transactions
from pyspark.sql.functions import col

# ---------------------------------------------------
# Step 2: Join Customers + Transactions (SILVER)
# ---------------------------------------------------

df_customer_tx = (
    df_tx.alias("t")
    .join(
        df_customers.alias("c"),
        col("t.customer_id") == col("c.customer_id"),
        "left"
    )
    .select(
        col("t.transaction_id"),
        col("t.customer_id"),         # primary key for grouping
        col("t.transaction_date"),
        col("t.payment_method"),
        col("t.status"),
        col("t.amount"),

        # Customer enrichments
        col("c.first_name"),
        col("c.last_name"),
        col("c.email"),
        col("c.phone"),
        col("c.city"),
        col("c.country"),
        col("c.signup_date"),
        col("c.age")
    )
)

df_customer_tx.show(5)
df_customer_tx.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Aggregations for GOLD (Transactions)

# COMMAND ----------

# DBTITLE 1,Main Metrics (Simple Aggregations)
from pyspark.sql.functions import (
    sum as _sum,
    count as _count,
    avg as _avg,
    min as _min,
    max as _max,
    when
)

# ------------------------------------------------------
# Step 3A: Core Customer Transaction Metrics (GOLD)
# ------------------------------------------------------

df_customer_tx_base = (
    df_customer_tx
        .groupBy("customer_id")
        .agg(
            _sum("amount").alias("total_transaction_amount"),
            _count("transaction_id").alias("transaction_count"),
            _sum(when(col("status") == "success", 1).otherwise(0)).alias("success_count"),
            _sum(when(col("status") == "failed", 1).otherwise(0)).alias("failed_count"),
            _min("transaction_date").alias("first_transaction_date"),
            _max("transaction_date").alias("last_transaction_date")
        )
)

df_customer_tx_base.show(10)
df_customer_tx_base.printSchema()


# COMMAND ----------

# DBTITLE 1,Count usage per payment method
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Step 3B-1: Count how many times each payment method is used per customer
df_payment_counts = (
    df_customer_tx
        .groupBy("customer_id", "payment_method")
        .agg(_count("payment_method").alias("method_count"))
)


# COMMAND ----------

# DBTITLE 1,Get the top method per customer
# Step 3B-2: Rank methods and pick the most used
window_spec = Window.partitionBy("customer_id").orderBy(col("method_count").desc())

df_top_method = (
    df_payment_counts
        .withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .select("customer_id", "payment_method")
        .withColumnRenamed("payment_method", "most_used_payment_method")
)


# COMMAND ----------

# DBTITLE 1,Join with the base metrics
# Step 3B-3: Join base metrics + most used payment method
df_customer_transactions_gold = (
    df_customer_tx_base
        .join(df_top_method, on="customer_id", how="left")
)

df_customer_transactions_gold.show(10)
df_customer_transactions_gold.printSchema()


# COMMAND ----------

# DBTITLE 1,WRITE GOLD TABLE TO DELTA
# ----------------------------------------------------
# Step 4: Write GOLD Customer Transactions Delta Table
# ----------------------------------------------------

gold_customer_tx_path = "/Volumes/workspace/default/gold_data/customer_transactions_gold"
#df_customer_transactions_gold.write.format("delta").mode("overwrite").save(gold_customer_tx_path)

df_customer_transactions_gold.write.format("delta").mode("overwrite").saveAsTable("default.gold_customer_transactions")
print("GOLD table 'customer_transactions_gold' created successfully!")


# COMMAND ----------

# DBTITLE 1,VALIDATE GOLD TABLE
df_customer_transactions_gold_check = spark.read.format("delta").load(gold_customer_tx_path)

df_customer_transactions_gold_check.show(10)
df_customer_transactions_gold_check.printSchema()
