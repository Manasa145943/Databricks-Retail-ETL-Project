# Databricks notebook source
# 04_data_quality.py — Quality gates across Bronze/Silver/Gold.
# Raises on CRITICAL failures so the Workflow task fails; WARNINGs are logged only.

dbutils.widgets.text("base_path", "/tmp/retail_etl", "Base working path")
base_path = dbutils.widgets.get("base_path").rstrip("/")

from pyspark.sql import functions as F

results = []

def check(name, severity, passed, detail=""):
    results.append({"check": name, "severity": severity,
                    "status": "PASS" if passed else "FAIL", "detail": detail})
    print(f"[{severity}] {name}: {'PASS' if passed else 'FAIL'} {detail}")

bronze_orders = spark.read.format("delta").load(f"{base_path}/bronze/orders")
silver_orders = spark.read.format("delta").load(f"{base_path}/silver/orders")
gold_cat = spark.read.format("delta").load(f"{base_path}/gold/daily_sales_by_category")

b_n, s_n = bronze_orders.count(), silver_orders.count()

# 1. Bronze is non-empty (critical)
check("bronze_orders_non_empty", "CRITICAL", b_n > 0, f"rows={b_n}")

# 2. Silver must not multiply rows (critical)
check("silver_le_bronze", "CRITICAL", 0 < s_n <= b_n, f"bronze={b_n} silver={s_n}")

# 3. No duplicate order_id in Silver (critical)
dupes = silver_orders.groupBy("order_id").count().filter("count > 1").count()
check("silver_no_duplicate_orders", "CRITICAL", dupes == 0, f"dupes={dupes}")

# 4. Null rates on key columns (warning at 5%)
for col in ["order_id", "customer_id", "order_date"]:
    rate = silver_orders.filter(F.col(col).isNull()).count() / max(s_n, 1)
    check(f"silver_null_rate_{col}", "WARNING", rate < 0.05, f"rate={rate:.4f}")

# 5. Gold reconciles to Silver within 0.1% (critical)
silver_total = silver_orders.agg(F.sum("line_total")).collect()[0][0] or 0
gold_total = gold_cat.agg(F.sum("gross_sales")).collect()[0][0] or 0
drift = abs(silver_total - gold_total) / max(silver_total, 1)
check("gold_reconciles_to_silver", "CRITICAL", drift < 0.001,
      f"silver={silver_total:.2f} gold={gold_total:.2f} drift={drift:.5f}")

critical_failures = [r for r in results if r["severity"] == "CRITICAL" and r["status"] == "FAIL"]
if critical_failures:
    raise Exception(f"DQ FAILED: {[r['check'] for r in critical_failures]}")

dbutils.notebook.exit(f"dq ok: {len(results)} checks passed")
