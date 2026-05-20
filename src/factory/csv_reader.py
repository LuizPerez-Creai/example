from pyspark.sql import DataFrame, SparkSession
from .base_reader import BaseReader


class CsvReader(BaseReader):
    def read(self, spark: SparkSession, path: str) -> DataFrame:
        return (
            spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(path)
        )