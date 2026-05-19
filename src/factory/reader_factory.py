from .base_reader import BaseReader
from .csv_reader import CsvReader
from .parquet_reader import ParquetReader
from .txt_reader import TxtReader


class SourceReaderFactory:
    """
    Factory for creating source readers.

    Open/Closed: register new formats with SourceReaderFactory.register()
    without touching existing reader classes.
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
        """Plug in a new source format at runtime without modifying this class."""
        cls._registry[source_type.lower()] = reader_cls

    @classmethod
    def supported_types(cls) -> list[str]:
        return list(cls._registry.keys())
