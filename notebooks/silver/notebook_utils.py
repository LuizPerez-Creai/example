# Databricks notebook source

# MAGIC %md
# MAGIC # Silver Utils — Reusable Transformations
# MAGIC Loaded into notebook_silver via `%run ./notebook_utils`.
# MAGIC All functions are pure DataFrame transforms — no DLT decorators here.

# COMMAND ----------
# DBTITLE 1,Type Casting
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def cast_columns(df: DataFrame, cast_map: dict[str, str]) -> DataFrame:
    """cast_map: {column_name: spark_type_string}  e.g. {"amount": "double"}"""
    for col_name, col_type in cast_map.items():
        df = df.withColumn(col_name, F.col(col_name).cast(col_type))
    return df


# COMMAND ----------
# DBTITLE 1,Null Handling

def drop_null_rows(df: DataFrame, subset: list[str] | None = None) -> DataFrame:
    return df.dropna(subset=subset)


def fill_nulls(df: DataFrame, fill_map: dict) -> DataFrame:
    return df.fillna(fill_map)


# COMMAND ----------
# DBTITLE 1,Column Renaming

def rename_columns(df: DataFrame, rename_map: dict[str, str]) -> DataFrame:
    for old_name, new_name in rename_map.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df


# COMMAND ----------
# DBTITLE 1,Deduplication

def deduplicate(
    df: DataFrame,
    keys: list[str],
    order_by: str | None = None,
) -> DataFrame:
    if order_by:
        from pyspark.sql.window import Window
        w = Window.partitionBy(keys).orderBy(F.col(order_by).desc())
        df = (
            df.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
    else:
        df = df.dropDuplicates(keys)
    return df


# COMMAND ----------
# DBTITLE 1,Composite Decorator

def apply_standard_transforms(
    df: DataFrame,
    cast_map: dict | None = None,
    fill_map: dict | None = None,
    rename_map: dict | None = None,
    dedup_keys: list[str] | None = None,
    dedup_order_by: str | None = None,
) -> DataFrame:
    """
    Chains all transforms in a fixed order.
    notebook_silver calls this single function per source — Decorator pattern:
    each step wraps the DataFrame without modifying the others.
    """
    if cast_map:
        df = cast_columns(df, cast_map)
    if fill_map:
        df = fill_nulls(df, fill_map)
    if rename_map:
        df = rename_columns(df, rename_map)
    if dedup_keys:
        df = deduplicate(df, dedup_keys, order_by=dedup_order_by)
    return df
