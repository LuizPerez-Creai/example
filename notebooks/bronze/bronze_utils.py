# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze Utils — Reader Classes & Factory
# MAGIC Loaded into notebook_bronze via `%run ./bronze_utils`.
# MAGIC Contains all reader classes and the factory in one place.

# COMMAND ----------
# DBTITLE 1,Base Reader
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession


class BaseReader(ABC):
    """Abstract base for all source readers. Add new formats by subclassing this."""

    @abstractmethod
    def read(self, spark: SparkSession, path: str) -> DataFrame:
        ...


# COMMAND ----------
# DBTITLE 1,CSV Reader
class CsvReader(BaseReader):
    def read(self, spark: SparkSession, path: str) -> DataFrame:
        return (
            spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(path)
        )


# COMMAND ----------
# DBTITLE 1,Parquet Reader
class ParquetReader(BaseReader):
    def read(self, spark: SparkSession, path: str) -> DataFrame:
        return spark.read.format("parquet").load(path)


# COMMAND ----------
# DBTITLE 1,TXT Reader
class TxtReader(BaseReader):
    def read(self, spark: SparkSession, path: str) -> DataFrame:
        return spark.read.format("text").load(path)


# COMMAND ----------
# DBTITLE 1,Source Reader Factory
class SourceReaderFactory:
    """
    Factory for creating source readers.
    Register new formats with SourceReaderFactory.register() without modifying this class.
    """

    _registry: dict[str, type[BaseReader]] = {
        "csv":     CsvReader,
        "parquet": ParquetReader,
        "txt":     TxtReader,
    }

    @classmethod
    def create(cls, source_type: str) -> BaseReader:
        reader_cls = cls._registry.get(source_type.lower())
        if reader_cls is None:
            raise ValueError(
                f"Unsupported source_type '{source_type}'. "
                f"Supported: {list(cls._registry.keys())}"
            )
        return reader_cls()

    @classmethod
    def register(cls, source_type: str, reader_cls: type[BaseReader]) -> None:
        """Plug in a new source format without modifying this class."""
        cls._registry[source_type.lower()] = reader_cls

    @classmethod
    def supported_types(cls) -> list[str]:
        return list(cls._registry.keys())