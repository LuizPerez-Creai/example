# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze Layer — Factory Pattern
# MAGIC Raw ingestion into Delta tables under governancecatalog.bronze.
# MAGIC One Delta table is written per entry in `bronze_params`. Runs on any cluster.

# COMMAND ----------
# DBTITLE 1,Load Utils
# MAGIC %run ./bronze_utils

# COMMAND ----------
# DBTITLE 1,Parameters
import json
import sys


def _safe_conf(key: str, default: str = "") -> str:
    # spark.conf.get() raises CONFIG_NOT_AVAILABLE in Spark Connect (DBR 13+)
    # when the key is not set, even if a default is provided. try/except is required.
    try:
        return spark.conf.get(key)
    except Exception:
        return default



_params_json = _safe_conf(
    "bronze_params",
    '[{"source_name":"customers","dlt_name":"raw_customers","source_type":"csv","schema":"bronze"},'
    '{"source_name":"orders","dlt_name":"raw_orders","source_type":"parquet","schema":"bronze"},'
    '{"source_name":"events","dlt_name":"raw_events","source_type":"txt","schema":"bronze"}]',
)
_base_path = _safe_conf("base_path", "/Volumes/governancecatalog/bronze/landing")
_repo_root = _safe_conf("repo_root", "/Workspace/Repos/luisperez@creai.mx/example")

PARAMS: list[dict] = json.loads(_params_json)

# COMMAND ----------
# DBTITLE 1,Factory — Ingest to Delta Tables


def _ingest_bronze_table(
    source_name: str,
    dlt_name: str,
    source_type: str,
    source_path: str,
    schema: str,
) -> None:
    target = f"governancecatalog.{schema}.{dlt_name}"
    df = SourceReaderFactory.create(source_type).read(spark, source_path)
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )
    print(f"[bronze] {target} — {df.count()} rows written")


for _p in PARAMS:
    _ingest_bronze_table(
        source_name = _p["source_name"],
        dlt_name    = _p["dlt_name"],
        source_type = _p["source_type"],
        source_path = f"{_base_path}/{_p['source_name']}",
        schema      = _p["schema"],
    )
