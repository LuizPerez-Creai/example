from pyspark.sql import DataFrame, SparkSession
from .base_reader import BaseReader


class TxtReader(BaseReader):
    def read(self, spark: SparkSession, path: str) -> DataFrame:
        return spark.read.format("text").load(path)