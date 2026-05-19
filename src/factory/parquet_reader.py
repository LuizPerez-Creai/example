from pyspark.sql import DataFrame, SparkSession
from .base_reader import BaseReader


class ParquetReader(BaseReader):
    def read(self, spark: SparkSession, path: str) -> DataFrame:
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", f"{path}/_schema")
            .load(path)
        )
