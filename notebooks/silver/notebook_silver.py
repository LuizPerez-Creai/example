# Databricks notebook source

# MAGIC %md
# MAGIC # Silver Layer — Decorator Pattern
# MAGIC Reads from bronze DLT tables, applies transformations via notebook_utils,
# MAGIC and writes to silver DLT tables. One table per entry in `pipeline.silver_params`.

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


_params_json = _safe_conf("pipeline.silver_params", "[]")
_repo_root   = _safe_conf("pipeline.repo_root")

PARAMS: list[dict] = json.loads(_params_json)

if _repo_root and _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

# COMMAND ----------
# DBTITLE 1,Load Utils (Decorator)
# MAGIC %run ./notebook_utils

# COMMAND ----------
# DBTITLE 1,Imports
import dlt
from pyspark.sql import DataFrame

# COMMAND ----------
# DBTITLE 1,Decorator — Dynamic Silver Table Registration
# TODO (next sprint): define per-source transform configs (cast_map, rename_map, etc.)
# and wire them to apply_standard_transforms below.


def _register_silver_table(dlt_source: str, dlt_name: str) -> None:
    @dlt.table(
        name=dlt_name,
        comment=f"Silver | source={dlt_source}",
        table_properties={"quality": "silver"},
    )
    def _transform() -> DataFrame:
        df = spark.readStream.table(dlt_source)
        df = apply_standard_transforms(df)   # extend with cast/rename/dedup args per source
        return df


for _p in PARAMS:
    _register_silver_table(
        dlt_source = _p["dlt_source"],
        dlt_name   = _p["dlt_destination_name"],
    )
