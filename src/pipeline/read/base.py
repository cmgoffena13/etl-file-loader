import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Iterator

from src.exception.exceptions import MissingColumnsError
from src.file_helper.base import BaseFileHelper
from src.file_helper.factory import FileHelperFactory
from src.settings import config
from src.sources.base import DataSource

logger = logging.getLogger(__name__)


class BaseReader(ABC):
    def __init__(self, file_path: Path, source: DataSource, log_id: int):
        self.file_path: Path = file_path
        self.source: DataSource = source
        self.log_id: int = log_id
        if not self.file_path.exists():
            raise FileNotFoundError(f"File not found: {self.file_path}")
        self.is_gzipped: bool = (
            len(file_path.suffixes) >= 2 and file_path.suffixes[-1].lower() == ".gz"
        )
        self.batch_size: int = config.BATCH_SIZE
        self.rows_read: int = 0
        self.file_helper: BaseFileHelper = FileHelperFactory.create_file_helper()

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
            logger.error(f"Missing columns: {missing_fields_display}")
            raise MissingColumnsError(
                error_values={
                    "source_filename": self.file_path.name,
                    "required_fields_display_formatted": ", ".join(
                        required_fields_display
                    ),
                    "missing_fields_display_formatted": ", ".join(
                        missing_fields_display
                    ),
                }
            )

    @abstractmethod
    def read(self) -> Iterator[Dict[str, Any]]:
        pass

    @property
    @abstractmethod
    def starting_row_number(self) -> int:
        pass

    def __iter__(self):
        return self.read()
