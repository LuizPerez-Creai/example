from pyspark.sql import DataFrame, SparkSession
from .base_reader import BaseReader


class CsvReader(BaseReader):
    def read(self, spark: SparkSession, path: str) -> DataFrame:
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation", f"{path}/_schema")
            .load(path)
        )
