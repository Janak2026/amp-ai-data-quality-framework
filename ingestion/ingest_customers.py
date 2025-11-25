# Databricks notebook source
# DBTITLE 1,LOAD
# Load RAW customers CSV into a DataFrame
raw_customers_path = "/Volumes/workspace/default/raw_data/customers.csv"

df_customers_raw = (
    spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(raw_customers_path)
)

df_customers_raw.show(5)
df_customers_raw.printSchema()


# COMMAND ----------

# DBTITLE 1,CLEANING & TRANSFORMATIONS
from pyspark.sql.functions import col, trim, lower

# -------------------------------
# Step 2: Cleaning Transformations
# -------------------------------

df_customers_clean = (
    df_customers_raw
        # Trim whitespace from all string fields
        .withColumn("customer_id", trim(col("customer_id")))
        .withColumn("first_name", trim(col("first_name")))
        .withColumn("last_name", trim(col("last_name")))
        .withColumn("email", lower(trim(col("email"))))  # lowercase email
        .withColumn("city", trim(col("city")))
        .withColumn("country", trim(col("country")))
        
        # Cast phone to string (important!)
        .withColumn("phone", col("phone").cast("string"))

        # Validate / cast age
        .withColumn("age", col("age").cast("integer"))
)

df_customers_clean.show(5)
df_customers_clean.printSchema()


# COMMAND ----------

# DBTITLE 1,SCHEMA ENFORCEMENT
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# ----------------------------------------------------
# Step 3: Define Strong Schema for Customers SILVER
# ----------------------------------------------------
customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("signup_date", DateType(), True),
    StructField("age", IntegerType(), True)
])

# Apply schema by selecting columns with cast
df_customers_silver = (
    df_customers_clean
        .select(
            col("customer_id").cast("string"),
            col("first_name").cast("string"),
            col("last_name").cast("string"),
            col("email").cast("string"),
            col("phone").cast("string"),
            col("city").cast("string"),
            col("country").cast("string"),
            col("signup_date").cast("date"),
            col("age").cast("integer")
        )
)

df_customers_silver.printSchema()
df_customers_silver.show(5)


# COMMAND ----------

# DBTITLE 1,WRITE SILVER DELTA TABLE
# ----------------------------------------------------
# Step 4: Write SILVER Customers Delta Table
# ----------------------------------------------------

silver_customers_path = "/Volumes/workspace/default/silver_data/customers_silver"

(
    df_customers_silver
        .write
        .format("delta")
        .mode("overwrite")
        .save(silver_customers_path)
)

print("Customers SILVER table created successfully!")


# COMMAND ----------

# DBTITLE 1,VALIDATE SILVER OUTPUT
# ----------------------------------------------------
# Step 5: Validate SILVER Output
# ----------------------------------------------------

df_customers_silver_check = spark.read.format("delta").load(silver_customers_path)

df_customers_silver_check.show(10)
df_customers_silver_check.printSchema()


# COMMAND ----------

