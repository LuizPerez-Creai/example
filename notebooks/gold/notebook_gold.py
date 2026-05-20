# Databricks notebook source

# MAGIC %md
# MAGIC # Gold Layer — SCD Type 2
# MAGIC Reads from silver Delta tables and applies SCD Type 2 via gold_utils.
# MAGIC Tracks isCurrent, effective_start, effective_end per record. Runs on any cluster.

# COMMAND ----------
# DBTITLE 1,Load Utils
# MAGIC %run ./gold_utils

# COMMAND ----------
# DBTITLE 1,Parameters
import json


def _safe_conf(key: str, default: str = "") -> str:
    # spark.conf.get() raises CONFIG_NOT_AVAILABLE in Spark Connect (DBR 13+)
    # when the key is not set, even if a default is provided. try/except is required.
    try:
        return spark.conf.get(key)
    except Exception:
        return default


_params_json = _safe_conf(
    "gold_params",
    '[{"dlt_source":"governancecatalog.silver.customers","dlt_destination_name":"dim_customers","schema":"gold","keys":["customer_id"]},'
    '{"dlt_source":"governancecatalog.silver.orders","dlt_destination_name":"dim_orders","schema":"gold","keys":["order_id"]}]',
)

PARAMS: list[dict] = json.loads(_params_json)

# COMMAND ----------
# DBTITLE 1,SCD Type 2 — Apply per Source


for _p in PARAMS:
    apply_scd2(
        spark        = spark,
        source_table = _p["dlt_source"],
        target_table = f"governancecatalog.{_p['schema']}.{_p['dlt_destination_name']}",
        keys         = _p["keys"],
    )