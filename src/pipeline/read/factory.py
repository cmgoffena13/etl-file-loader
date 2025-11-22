from pathlib import Path
from typing import Type, Union

from src.pipeline.read.base import BaseReader
from src.pipeline.read.csv import CSVReader
from src.pipeline.read.excel import ExcelReader
from src.pipeline.read.json import JSONReader
from src.sources.base import DataSource
from src.utils import get_file_extension


class ReaderFactory:
    _readers = {
        ".csv": CSVReader,
        ".xlsx": ExcelReader,
        ".xls": ExcelReader,
        ".json": JSONReader,
        ".csv.gz": CSVReader,
        ".json.gz": JSONReader,
    }

    @classmethod
    def get_supported_extensions(cls) -> list[str]:
        return list[str](cls._readers.keys())

    @classmethod
    def create_reader(
        cls, file_path: Union[Path, str], source: Type[DataSource], log_id: int
    ) -> Type[BaseReader]:
        extension = get_file_extension(file_path)

        reader_class = cls._readers[extension]

        reader_kwargs = source.model_dump(
            include={"delimiter", "encoding", "skip_rows", "sheet_name", "array_path"}
        )

        return reader_class(
            file_path=file_path, source=source, log_id=log_id, **reader_kwargs
        )
