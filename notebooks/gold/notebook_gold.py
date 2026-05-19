# Databricks notebook source

# MAGIC %md
# MAGIC # Gold Layer — SCD Type 2
# MAGIC Applies native DLT `apply_changes` with `stored_as_scd_type="2"` to silver tables.
# MAGIC Adds `isCurrent`, `__START_AT`, `__END_AT` columns automatically via DLT SCD2 API.

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


_params_json = _safe_conf("pipeline.gold_params", "[]")
_repo_root   = _safe_conf("pipeline.repo_root")

PARAMS: list[dict] = json.loads(_params_json)

if _repo_root and _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

# COMMAND ----------
# DBTITLE 1,Imports
import dlt
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------
# DBTITLE 1,SCD Type 2 — Dynamic Table Registration
# DLT SCD2 requires two steps per target table:
#   1. dlt.create_streaming_table()  — declares the target
#   2. dlt.apply_changes()           — defines the merge/SCD2 logic
#
# TODO (next sprint): define per-source keys and sequence_by columns.
# Placeholder keys below must be replaced with the actual primary key per source.


def _register_scd2_table(dlt_source: str, dlt_name: str) -> None:
    # dlt.apply_changes() requires a DLT-internal source name, not a fully-qualified
    # external path. Wrapping the cross-pipeline silver table in a @dlt.view makes it
    # visible to apply_changes() within this pipeline's scope.
    view_name = f"_src_{dlt_name}"

    @dlt.view(name=view_name)
    def _source_stream():
        return spark.readStream.table(dlt_source)

    dlt.create_streaming_table(
        name=dlt_name,
        comment=f"Gold SCD2 | source={dlt_source}",
        table_properties={
            "quality": "gold",
            "pipelines.autoOptimize.managed": "true",
        },
    )

    dlt.apply_changes(
        target             = dlt_name,
        source             = view_name,
        keys               = ["id"],             # TODO: set real primary key per source
        sequence_by        = F.col("updated_at"), # TODO: set real sequence column
        stored_as_scd_type = "2",
    )


for _p in PARAMS:
    _register_scd2_table(
        dlt_source = _p["dlt_source"],
        dlt_name   = _p["dlt_destination_name"],
    )
