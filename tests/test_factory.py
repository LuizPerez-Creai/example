import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.factory.reader_factory import SourceReaderFactory
from src.factory.base_reader import BaseReader
from src.factory.csv_reader import CsvReader
from src.factory.parquet_reader import ParquetReader
from src.factory.txt_reader import TxtReader


class TestSourceReaderFactory:

    def test_creates_csv_reader(self):
        assert isinstance(SourceReaderFactory.create("csv"), CsvReader)

    def test_creates_parquet_reader(self):
        assert isinstance(SourceReaderFactory.create("parquet"), ParquetReader)

    def test_creates_txt_reader(self):
        assert isinstance(SourceReaderFactory.create("txt"), TxtReader)

    def test_case_insensitive(self):
        assert isinstance(SourceReaderFactory.create("CSV"), CsvReader)
        assert isinstance(SourceReaderFactory.create("Parquet"), ParquetReader)
        assert isinstance(SourceReaderFactory.create("TXT"), TxtReader)

    def test_raises_on_unknown_type(self):
        with pytest.raises(ValueError, match="Unsupported source_type"):
            SourceReaderFactory.create("xml")

    def test_register_new_type(self):
        class JsonReader(BaseReader):
            def read(self, spark, path):
                pass

        SourceReaderFactory.register("json", JsonReader)
        assert isinstance(SourceReaderFactory.create("json"), JsonReader)
        del SourceReaderFactory._registry["json"]  # cleanup

    def test_supported_types_contains_defaults(self):
        types = SourceReaderFactory.supported_types()
        assert "csv" in types
        assert "parquet" in types
        assert "txt" in types

    def test_register_does_not_affect_existing_types(self):
        class XmlReader(BaseReader):
            def read(self, spark, path):
                pass

        SourceReaderFactory.register("xml", XmlReader)
        assert isinstance(SourceReaderFactory.create("csv"), CsvReader)
        del SourceReaderFactory._registry["xml"]  # cleanup
