# Run this script locally or in a Databricks notebook to generate orders.parquet
# Requires: pip install pandas pyarrow

import pandas as pd
import os

data = [
    {"order_id": 5001, "customer_id": 1, "product_id": 101, "quantity": 2, "unit_price": 49.99, "status": "delivered", "created_at": "2024-01-15 08:35:00", "updated_at": "2024-01-17 12:00:00"},
    {"order_id": 5002, "customer_id": 4, "product_id": 205, "quantity": 1, "unit_price": 120.00, "status": "shipped",   "created_at": "2024-01-18 11:50:00", "updated_at": "2024-01-19 09:00:00"},
    {"order_id": 5003, "customer_id": 2, "product_id": 310, "quantity": 3, "unit_price": 15.50,  "status": "pending",   "created_at": "2024-01-20 10:00:00", "updated_at": "2024-01-20 10:00:00"},
    {"order_id": 5004, "customer_id": 6, "product_id": 101, "quantity": 1, "unit_price": 49.99,  "status": "delivered", "created_at": "2024-01-21 14:30:00", "updated_at": "2024-01-23 08:00:00"},
    {"order_id": 5005, "customer_id": 3, "product_id": 410, "quantity": 2, "unit_price": 75.00,  "status": "cancelled", "created_at": "2024-01-22 09:15:00", "updated_at": "2024-01-22 15:00:00"},
    {"order_id": 5006, "customer_id": 5, "product_id": 205, "quantity": 1, "unit_price": 120.00, "status": "shipped",   "created_at": "2024-01-23 11:00:00", "updated_at": "2024-01-24 10:00:00"},
    {"order_id": 5007, "customer_id": 9, "product_id": 310, "quantity": 4, "unit_price": 15.50,  "status": "pending",   "created_at": "2024-01-24 13:45:00", "updated_at": "2024-01-24 13:45:00"},
    {"order_id": 5001, "customer_id": 1, "product_id": 101, "quantity": 2, "unit_price": 49.99,  "status": "delivered", "created_at": "2024-01-15 08:35:00", "updated_at": "2024-01-17 12:00:00"},
]

df = pd.DataFrame(data)
output_path = os.path.join(os.path.dirname(__file__), "orders.parquet")
df.to_parquet(output_path, index=False, engine="pyarrow")
print(f"Written: {output_path}")
