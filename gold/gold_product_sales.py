# Databricks notebook source
# DBTITLE 1,Load SILVER Tables
# Load SILVER Sales
sales_silver_path = "/Volumes/workspace/default/silver_data/sales_silver"
df_sales = spark.read.format("delta").load(sales_silver_path)

df_sales.show(5)
df_sales.printSchema()


# COMMAND ----------

# DBTITLE 1,Product-Level Aggregations
from pyspark.sql.functions import (
    sum as _sum,
    avg as _avg,
    min as _min,
    max as _max
)

# ------------------------------------------------------
# Step 2: Product-Level Aggregation (GOLD)
# ------------------------------------------------------

df_product_sales_gold = (
    df_sales
        .groupBy("product_id")
        .agg(
            _sum("quantity").alias("total_quantity_sold"),
            _sum("amount").alias("total_revenue"),
            _avg("amount").alias("avg_price"),
            _min("sale_date").alias("first_sale_date"),
            _max("sale_date").alias("last_sale_date")
        )
)

df_product_sales_gold.show(10)
df_product_sales_gold.printSchema()


# COMMAND ----------

# DBTITLE 1,WRITE GOLD TABLE TO DELTA
# ----------------------------------------------------
# Step 3: Write GOLD Product Sales Delta Table
# ----------------------------------------------------

gold_product_sales_path = "/Volumes/workspace/default/gold_data/product_sales_gold"
#df_product_sales_gold.write.format("delta").mode("overwrite").save(gold_product_sales_path)

df_product_sales_gold.write.format("delta").mode("overwrite").saveAsTable("default.gold_product_sales")
print("GOLD table 'product_sales_gold' created successfully!")


# COMMAND ----------

# DBTITLE 1,VALIDATE GOLD TABLE
df_product_sales_gold_check = spark.read.format("delta").load(gold_product_sales_path)

df_product_sales_gold_check.show(10)
df_product_sales_gold_check.printSchema()
