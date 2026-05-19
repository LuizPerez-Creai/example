# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze Layer — Factory Pattern
# MAGIC Raw ingestion from cloud storage using Auto Loader (cloudFiles).
# MAGIC One DLT streaming table is created per entry in `pipeline.bronze_params`.

# COMMAND ----------
# DBTITLE 1,Parameters
import json
import sys

# Pipeline parameters are set in databricks.yml under resources.pipelines.*.configuration
# Pass the JSON array as a single string value; it is parsed here.
_params_json = spark.conf.get("pipeline.bronze_params", "[]")
_base_path   = spark.conf.get("pipeline.base_path", "")
_repo_root   = spark.conf.get("pipeline.repo_root", "")

PARAMS: list[dict] = json.loads(_params_json)

# COMMAND ----------
# DBTITLE 1,Imports
# Add repo root to sys.path so src.factory is importable inside DLT
if _repo_root and _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

import dlt
from pyspark.sql import DataFrame
from src.factory.reader_factory import SourceReaderFactory

# COMMAND ----------
# DBTITLE 1,Factory — Dynamic DLT Table Registration


def _register_bronze_table(
    source_name: str,
    dlt_name: str,
    source_type: str,
    source_path: str,
) -> None:
    """
    Wraps @dlt.table in a closure so each iteration captures its own
    parameter values. Without the closure, all tables would share the
    last value of the loop variable (late-binding bug).
    """

    @dlt.table(
        name=dlt_name,
        comment=f"Bronze | source={source_name} | format={source_type}",
        table_properties={
            "quality": "bronze",
            "pipelines.autoOptimize.managed": "true",
        },
    )
    def _ingest() -> DataFrame:
        return SourceReaderFactory.create(source_type).read(spark, source_path)


for _p in PARAMS:
    _register_bronze_table(
        source_name = _p["source_name"],
        dlt_name    = _p["dlt_name"],
        source_type = _p["source_type"],
        source_path = f"{_base_path}/{_p['source_name']}",
    )
