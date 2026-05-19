from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession


class BaseReader(ABC):
    """Abstract base for all source readers. Extend, never modify."""

    @abstractmethod
    def read(self, spark: SparkSession, path: str) -> DataFrame:
        """Return a streaming DataFrame suitable for DLT Auto Loader ingestion."""
        ...
