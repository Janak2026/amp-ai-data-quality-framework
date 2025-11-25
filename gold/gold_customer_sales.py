# Databricks notebook source
# DBTITLE 1,Load SILVER Tables
# Load SILVER Customers
customers_silver_path = "/Volumes/workspace/default/silver_data/customers_silver"
df_customers = spark.read.format("delta").load(customers_silver_path)

# Load SILVER Sales
sales_silver_path = "/Volumes/workspace/default/silver_data/sales_silver"
df_sales = spark.read.format("delta").load(sales_silver_path)

df_customers.show(3)
df_sales.show(3)


# COMMAND ----------

# DBTITLE 1,Join Customers + Sales
from pyspark.sql.functions import col

# ---------------------------------------------
# Step 3 (Fixed): Join Customers + Sales Cleanly
# ---------------------------------------------
df_customer_sales = (
    df_sales.alias("s")
    .join(
        df_customers.alias("c"),
        col("s.customer_id") == col("c.customer_id"),
        "left"
    )
    # Remove duplicate customer_id from customers side
    .select(
        col("s.sale_id"),
        col("s.customer_id"),         # sales customer_id is primary
        col("s.product_id"),
        col("s.sale_date"),
        col("s.quantity"),
        col("s.amount"),

        # customer fields (no ambiguity now)
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

df_customer_sales.show(5)
df_customer_sales.printSchema()


# COMMAND ----------

# DBTITLE 1,Aggregate to Build GOLD Metrics
from pyspark.sql.functions import (
    sum as _sum,
    count as _count,
    avg as _avg,
    min as _min,
    max as _max
)

# -------------------------------------------------
# Step 4: Customer-Level Aggregations (GOLD)
# -------------------------------------------------

df_customer_sales_gold = (
    df_customer_sales
        .groupBy("customer_id")
        .agg(
            _sum("amount").alias("total_spend"),
            _count("sale_id").alias("total_orders"),
            _avg("amount").alias("avg_order_value"),
            _min("sale_date").alias("first_purchase_date"),
            _max("sale_date").alias("last_purchase_date")
        )
)

df_customer_sales_gold.show(10)
df_customer_sales_gold.printSchema()


# COMMAND ----------

# DBTITLE 1,WRITE GOLD TABLE TO DELTA
# ----------------------------------------------------
# Step 5: Write GOLD Customer Sales Delta Table
# ----------------------------------------------------

gold_customer_sales_path = "/Volumes/workspace/default/gold_data/customer_sales_gold"
#df_customer_sales_gold.write.format("delta").mode("overwrite").save(gold_customer_sales_path)

df_customer_sales_gold.write.format("delta").mode("overwrite").saveAsTable("default.gold_customer_sales")
print("GOLD table 'customer_sales_gold' created successfully!")


# COMMAND ----------

# DBTITLE 1,VALIDATE GOLD TABLE
df_customer_sales_gold_check = spark.read.format("delta").load(gold_customer_sales_path)

df_customer_sales_gold_check.show(10)
df_customer_sales_gold_check.printSchema()
