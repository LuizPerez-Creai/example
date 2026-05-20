# Databricks notebook source

# MAGIC %md
# MAGIC # Gold Utils — SCD Type 2 Logic
# MAGIC Loaded into notebook_gold via `%run ./gold_utils`.
# MAGIC Implements SCD2 using Delta MERGE without DLT.

# COMMAND ----------
# DBTITLE 1,SCD Type 2
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def apply_scd2(
    spark: SparkSession,
    source_table: str,
    target_table: str,
    keys: list[str],
) -> None:
    """
    Applies SCD Type 2 from source_table into target_table.

    Adds three metadata columns:
      - isCurrent      (boolean)  : True for the active version of a record
      - effective_start (timestamp): when this version became active
      - effective_end   (timestamp): when this version was superseded (null = still active)
    """

    df_source = spark.read.table(source_table)
    now = F.current_timestamp()

    # Enrich incoming records with SCD2 metadata
    df_incoming = (
        df_source
        .withColumn("isCurrent",       F.lit(True))
        .withColumn("effective_start", now)
        .withColumn("effective_end",   F.lit(None).cast("timestamp"))
    )

    # First load: table does not exist yet — write all as current
    if not spark.catalog.tableExists(target_table):
        (
            df_incoming.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(target_table)
        )
        print(f"[gold] {target_table} — initial load, {df_source.count()} records")
        return

    # Incremental run — detect changes and apply SCD2 MERGE
    scd2_meta  = {"isCurrent", "effective_start", "effective_end"}
    data_cols  = [c for c in df_source.columns if c not in set(keys) and c not in scd2_meta]

    key_join    = " AND ".join([f"t.{k} = s.{k}"   for k in keys])
    change_expr = " OR ".join([f"t.{c} <> s.{c}"   for c in data_cols]) if data_cols else "1=0"

    delta_tbl = DeltaTable.forName(spark, target_table)

    # Step 1: expire current records whose data changed
    delta_tbl.alias("t").merge(
        df_incoming.alias("s"),
        f"{key_join} AND t.isCurrent = true AND ({change_expr})",
    ).whenMatchedUpdate(set={
        "isCurrent":     F.lit(False),
        "effective_end": F.col("s.effective_start"),
    }).execute()

    # Step 2: insert records that are not currently active (new keys or just expired)
    df_active_keys = (
        spark.read.table(target_table)
        .filter(F.col("isCurrent") == True)
        .select(keys)
    )
    df_to_insert = df_incoming.join(df_active_keys, keys, "left_anti")

    if df_to_insert.count() > 0:
        (
            df_to_insert.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(target_table)
        )

    print(f"[gold] {target_table} — SCD2 applied, {df_to_insert.count()} new versions inserted")
