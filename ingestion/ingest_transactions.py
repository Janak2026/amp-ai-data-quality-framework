# Databricks notebook source
# DBTITLE 1,LOAD
# Load RAW transactions CSV into DataFrame
raw_transactions_path = "/Volumes/workspace/default/raw_data/transactions.csv"

df_tx_raw = (
    spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(raw_transactions_path)
)

df_tx_raw.show(5)
df_tx_raw.printSchema()


# COMMAND ----------

# DBTITLE 1,CLEANING & TRANSFORMATIONS
from pyspark.sql.functions import col, trim, lower

# ---------------------------------------------
# Step 2: Clean and Standardize Transactions Data
# ---------------------------------------------

df_tx_clean = (
    df_tx_raw
        .withColumn("transaction_id", trim(col("transaction_id")))
        .withColumn("customer_id", trim(col("customer_id")))
        .withColumn("transaction_date", col("transaction_date").cast("date"))
        
        # Normalize payment method and status (lowercase + trim)
        .withColumn("payment_method", lower(trim(col("payment_method"))))
        .withColumn("status", lower(trim(col("status"))))
        
        # Cast amount to integer
        .withColumn("amount", col("amount").cast("integer"))
        
        # Remove duplicates
        .dropDuplicates()
)

df_tx_clean.show(5)
df_tx_clean.printSchema()


# COMMAND ----------

# DBTITLE 1,SCHEMA ENFORCEMENT
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col

# -----------------------------------------
# Step 3: Define Strong Schema for Transactions
# -----------------------------------------
transactions_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("transaction_date", DateType(), True),
    StructField("payment_method", StringType(), True),
    StructField("status", StringType(), True),
    StructField("amount", IntegerType(), True)
])

# Apply schema by casting each column
df_tx_silver = (
    df_tx_clean
        .select(
            col("transaction_id").cast("string"),
            col("customer_id").cast("string"),
            col("transaction_date").cast("date"),
            col("payment_method").cast("string"),
            col("status").cast("string"),
            col("amount").cast("integer")
        )
)

df_tx_silver.printSchema()
df_tx_silver.show(5)


# COMMAND ----------

# DBTITLE 1,WRITE SILVER DELTA TABLE
# ----------------------------------------------------
# Step 4: Write SILVER Transactions Delta Table
# ----------------------------------------------------

silver_tx_path = "/Volumes/workspace/default/silver_data/transactions_silver"

(
    df_tx_silver
        .write
        .format("delta")
        .mode("overwrite")
        .save(silver_tx_path)
)

print("Transactions SILVER table created successfully!")


# COMMAND ----------

# DBTITLE 1,VALIDATE SILVER OUTPUT
# ----------------------------------------------------
# Step 5: Validate SILVER Output
# ----------------------------------------------------

df_tx_silver_check = spark.read.format("delta").load(silver_tx_path)

df_tx_silver_check.show(10)
df_tx_silver_check.printSchema()
