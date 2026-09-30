# Databricks notebook source
# 01_bronze_ingest.py — Raw ingestion: CSV source files -> Delta Bronze with lineage columns.

dbutils.widgets.text("base_path", "/tmp/retail_etl", "Base working path")
dbutils.widgets.text("data_path", "/tmp/retail_etl/source", "Source CSV directory")

base_path = dbutils.widgets.get("base_path").rstrip("/")
data_path = dbutils.widgets.get("data_path").rstrip("/")

from pyspark.sql import functions as F

def ingest(csv_name: str, table: str):
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{data_path}/{csv_name}")
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )
    out = f"{base_path}/bronze/{table}"
    df.write.format("delta").mode("overwrite").save(out)
    n = spark.read.format("delta").load(out).count()
    print(f"bronze/{table}: {n} rows -> {out}")
    return n

orders_n = ingest("retail_orders.csv", "orders")
customers_n = ingest("retail_customers.csv", "customers")

dbutils.notebook.exit(f"bronze ok: orders={orders_n}, customers={customers_n}")
