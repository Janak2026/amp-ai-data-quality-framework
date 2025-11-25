# Databricks notebook source
# Load RAW sales CSV into DataFrame
raw_sales_path = "/Volumes/workspace/default/raw_data/sales.csv"

df_sales_raw = (
    spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(raw_sales_path)
)

df_sales_raw.show(5)
df_sales_raw.printSchema()


# COMMAND ----------

# DBTITLE 1,CLEANING & TRANSFORMATIONS
from pyspark.sql.functions import col, trim

# ---------------------------------------
# Step 2: Clean and Standardize Sales Data
# ---------------------------------------

df_sales_clean = (
    df_sales_raw
        .withColumn("sale_id", trim(col("sale_id")))
        .withColumn("customer_id", trim(col("customer_id")))
        .withColumn("product_id", trim(col("product_id")))
        .withColumn("sale_date", col("sale_date").cast("date"))
        .withColumn("quantity", col("quantity").cast("integer"))
        .withColumn("amount", col("amount").cast("integer"))
        .dropDuplicates()
)

df_sales_clean.show(5)
df_sales_clean.printSchema()


# COMMAND ----------

# DBTITLE 1,SCHEMA ENFORCEMENT
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col

# -----------------------------------------
# Step 3: Define Strong Schema for Sales
# -----------------------------------------
sales_schema = StructType([
    StructField("sale_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("sale_date", DateType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("amount", IntegerType(), True)
])

# Apply schema by selecting with casting
df_sales_silver = (
    df_sales_clean
        .select(
            col("sale_id").cast("string"),
            col("customer_id").cast("string"),
            col("product_id").cast("string"),
            col("sale_date").cast("date"),
            col("quantity").cast("integer"),
            col("amount").cast("integer")
        )
)

df_sales_silver.printSchema()
df_sales_silver.show(5)


# COMMAND ----------

# DBTITLE 1,WRITE SILVER DELTA TABLE
# ----------------------------------------------------
# Step 4: Write SILVER Sales Delta Table
# ----------------------------------------------------

silver_sales_path = "/Volumes/workspace/default/silver_data/sales_silver"

(
    df_sales_silver
        .write
        .format("delta")
        .mode("overwrite")
        .save(silver_sales_path)
)

print("Sales SILVER table created successfully!")


# COMMAND ----------

# DBTITLE 1,VALIDATE SILVER OUTPUT
# ----------------------------------------------------
# Step 5: Validate SILVER Output
# ----------------------------------------------------

df_sales_silver_check = spark.read.format("delta").load(silver_sales_path)

df_sales_silver_check.show(10)
df_sales_silver_check.printSchema()
