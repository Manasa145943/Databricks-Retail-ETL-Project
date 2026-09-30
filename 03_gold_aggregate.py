# Databricks notebook source
# 03_gold_aggregate.py — Silver -> Gold marts: daily sales by category/region + top products.

dbutils.widgets.text("base_path", "/tmp/retail_etl", "Base working path")
base_path = dbutils.widgets.get("base_path").rstrip("/")

from pyspark.sql import functions as F

orders = spark.read.format("delta").load(f"{base_path}/silver/orders")
customers = spark.read.format("delta").load(f"{base_path}/silver/customers")

enriched = orders.join(customers, on="customer_id", how="left")

daily_by_category = (
    enriched.groupBy("order_date", "category")
    .agg(
        F.countDistinct("order_id").alias("orders"),
        F.sum("quantity").alias("units_sold"),
        F.round(F.sum("line_total"), 2).alias("gross_sales"),
    )
)

daily_by_region = (
    enriched.groupBy("order_date", "region")
    .agg(
        F.countDistinct("order_id").alias("orders"),
        F.round(F.sum("line_total"), 2).alias("gross_sales"),
        F.countDistinct("customer_id").alias("active_customers"),
    )
)

top_products = (
    enriched.groupBy("product_name", "category")
    .agg(
        F.sum("quantity").alias("units_sold"),
        F.round(F.sum("line_total"), 2).alias("gross_sales"),
    )
    .orderBy(F.desc("gross_sales"))
    .limit(50)
)

daily_by_category.write.format("delta").mode("overwrite") \
    .save(f"{base_path}/gold/daily_sales_by_category")
daily_by_region.write.format("delta").mode("overwrite") \
    .save(f"{base_path}/gold/daily_sales_by_region")
top_products.write.format("delta").mode("overwrite") \
    .save(f"{base_path}/gold/top_products")

print("gold tables written:")
for t in ["daily_sales_by_category", "daily_sales_by_region", "top_products"]:
    n = spark.read.format("delta").load(f"{base_path}/gold/{t}").count()
    print(f"  gold/{t}: {n} rows")

dbutils.notebook.exit("gold ok")
