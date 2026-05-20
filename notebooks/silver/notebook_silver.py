# Databricks notebook source

# MAGIC %md
# MAGIC # Silver Layer — Decorator Pattern
# MAGIC Reads from bronze Delta tables, applies transformations via notebook_utils,
# MAGIC and writes to silver Delta tables. Runs on any cluster (no DLT required).

# COMMAND ----------
# DBTITLE 1,Load Utils (Decorator)
# MAGIC %run ./notebook_utils

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
    "silver_params",
    '[{"dlt_source":"governancecatalog.bronze.raw_customers","dlt_destination_name":"customers","schema":"silver"},'
    '{"dlt_source":"governancecatalog.bronze.raw_orders","dlt_destination_name":"orders","schema":"silver"},'
    '{"dlt_source":"governancecatalog.bronze.raw_events","dlt_destination_name":"events","schema":"silver"}]',
)

PARAMS: list[dict] = json.loads(_params_json)

# COMMAND ----------
# DBTITLE 1,Decorator — Ingest to Silver Delta Tables


def _transform_silver_table(source: str, destination: str, schema: str) -> None:
    target = f"governancecatalog.{schema}.{destination}"
    df = spark.read.table(source)
    df = apply_standard_transforms(df)
    (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(target)
    )
    print(f"[silver] {target} — {df.count()} rows written")


for _p in PARAMS:
    _transform_silver_table(
        source      = _p["dlt_source"],
        destination = _p["dlt_destination_name"],
        schema      = _p["schema"],
    )