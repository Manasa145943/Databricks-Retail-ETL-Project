#!/usr/bin/env python3
"""Generate synthetic retail data for the Databricks medallion ETL demo.

Run: python generate_retail_data.py --orders 50000 --customers 5000 --out ./source
Output: retail_orders.csv, retail_customers.csv
"""
import argparse
import csv
import os
import random
from datetime import date, timedelta

random.seed(42)

CATEGORIES = {
    "Electronics": [("Laptop Pro 14", 1299.0), ("Wireless Headphones", 199.0),
                    ("Smartphone X", 899.0), ("4K Monitor 27in", 349.0)],
    "Home & Kitchen": [("Air Fryer XL", 129.0), ("Robot Vacuum", 299.0),
                       ("Espresso Machine", 449.0), ("Cookware Set", 189.0)],
    "Apparel": [("Running Shoes", 139.0), ("Denim Jacket", 89.0),
                ("Winter Parka", 249.0), ("Yoga Leggings", 59.0)],
    "Sports": [("Mountain Bike", 799.0), ("Dumbbell Set", 149.0),
               ("Camping Tent 4P", 229.0), ("Pickleball Paddle", 79.0)],
    "Beauty": [("Vitamin C Serum", 39.0), ("Hair Dryer Pro", 159.0),
               ("Skincare Set", 99.0), ("Eau de Parfum", 120.0)],
}
REGIONS = ["North", "South", "East", "West", "Central"]
CHANNELS = ["web", "mobile", "store"]
FIRST = ["Ava", "Liam", "Maya", "Noah", "Zoe", "Ethan", "Priya", "Lucas",
         "Nina", "Omar", "Tara", "Vikram", "Sara", "Dev", "Ivy"]
LAST = ["Sharma", "Patel", "Garcia", "Kim", "Nguyen", "Johnson", "Smith",
        "Brown", "Davis", "Miller", "Rao", "Khan", "Ali", "Chen", "Lopez"]


def gen_customers(n):
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "customer_id": f"C{i:06d}",
            "first_name": random.choice(FIRST),
            "last_name": random.choice(LAST),
            "email": f"user{i}@example.com",
            "region": random.choice(REGIONS),
            "signup_date": (date(2022, 1, 1) + timedelta(days=random.randint(0, 900))).isoformat(),
        })
    return rows


def gen_orders(n, customers):
    rows = []
    start = date(2024, 1, 1)
    for i in range(1, n + 1):
        cat = random.choice(list(CATEGORIES.keys()))
        product, price = random.choice(CATEGORIES[cat])
        qty = random.choices([1, 2, 3, 4, 5], weights=[70, 15, 8, 4, 3])[0]
        order_date = start + timedelta(days=random.randint(0, 364))
        # sprinkle in a little messiness for the DQ layer to catch
        cust = random.choice(customers)["customer_id"]
        if random.random() < 0.005:
            cust = None  # orphan order
        rows.append({
            "order_id": f"O{i:07d}",
            "customer_id": cust,
            "product_name": product,
            "category": cat,
            "quantity": qty,
            "unit_price": price,
            "order_date": order_date.isoformat(),
            "channel": random.choice(CHANNELS),
            "region": random.choice(REGIONS),
        })
    # a few exact duplicates to exercise dedup logic
    rows.extend(random.sample(rows, k=max(1, n // 500)))
    random.shuffle(rows)
    return rows


def write_csv(path, rows):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--orders", type=int, default=50000)
    ap.add_argument("--customers", type=int, default=5000)
    ap.add_argument("--out", default="./source")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    customers = gen_customers(a.customers)
    orders = gen_orders(a.orders, customers)
    write_csv(os.path.join(a.out, "retail_customers.csv"), customers)
    write_csv(os.path.join(a.out, "retail_orders.csv"), orders)
    print(f"Wrote {len(customers)} customers and {len(orders)} orders to {a.out}/")


if __name__ == "__main__":
    main()
