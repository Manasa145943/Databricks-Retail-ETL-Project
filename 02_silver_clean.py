# Databricks notebook source
# 02_silver_clean.py — Clean, dedupe, type-cast Bronze -> Silver (partitioned by order_date).

dbutils.widgets.text("base_path", "/tmp/retail_etl", "Base working path")
base_path = dbutils.widgets.get("base_path").rstrip("/")

from pyspark.sql import functions as F
from pyspark.sql.types import DateType

orders = spark.read.format("delta").load(f"{base_path}/bronze/orders")
customers = spark.read.format("delta").load(f"{base_path}/bronze/customers")

# --- orders: dedupe, cast, validate ---
silver_orders = (
    orders
    .dropDuplicates(["order_id"])
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("double"))
    .withColumn("region", F.upper(F.trim("region")))
    .withColumn("channel", F.lower(F.trim("channel")))
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("quantity") > 0)
    .filter(F.col("unit_price") >= 0)
    .filter(F.col("order_date").isNotNull())
    .withColumn("line_total", F.round(F.col("quantity") * F.col("unit_price"), 2))
)

# --- customers: dedupe, cast, standardize ---
silver_customers = (
    customers
    .dropDuplicates(["customer_id"])
    .withColumn("signup_date", F.to_date("signup_date"))
    .withColumn("region", F.upper(F.trim("region")))
    .filter(F.col("customer_id").isNotNull())
)

silver_orders.write.format("delta").mode("overwrite") \
    .partitionBy("order_date").save(f"{base_path}/silver/orders")
silver_customers.write.format("delta").mode("overwrite") \
    .save(f"{base_path}/silver/customers")

print(f"silver/orders: {silver_orders.count()} rows")
print(f"silver/customers: {silver_customers.count()} rows")

dbutils.notebook.exit("silver ok")
