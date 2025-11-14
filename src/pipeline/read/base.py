from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Iterator

from src.exceptions import MissingColumnsError
from src.settings import config
from src.sources.base import DataSource


class BaseReader(ABC):
    def __init__(self, file_path: Path, source: DataSource):
        self.file_path = file_path
        self.source = source
        if not self.file_path.exists():
            raise FileNotFoundError(f"File not found: {self.file_path}")
        self.is_gzipped = (
            len(file_path.suffixes) >= 2 and file_path.suffixes[-1].lower() == ".gz"
        )
        self.batch_size = config.BATCH_SIZE
        self.total_rows = 0

    def _validate_fields(self, actual_fields: set) -> None:
        actual_file_fields = set(field.lower() for field in actual_fields)
        required_file_fields = set(
            field.alias.lower() if field.alias else name.lower()
            for name, field in self.source.source_model.model_fields.items()
        )
        missing_fields = required_file_fields - actual_file_fields

        if missing_fields:
            required_fields_display = sorted(required_file_fields)
            missing_fields_display = sorted(missing_fields)

            error_msg = (
                f"Missing required fields in {self.file_path.suffix.upper()} file {self.file_path.name}\n"
                f"Required fields: {', '.join(required_fields_display)}\n"
                f"Missing fields: {', '.join(missing_fields_display)}"
            )
            raise MissingColumnsError(error_msg)

    @abstractmethod
    def read(self) -> Iterator[Dict[str, Any]]:
        pass

    @property
    @abstractmethod
    def starting_row_number(self) -> int:
        pass

    def __iter__(self):
        return self.read()
