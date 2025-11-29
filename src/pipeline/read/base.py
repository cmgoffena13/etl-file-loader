import gzip
import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, Union

from src.exception.exceptions import MissingColumnsError
from src.file_helper.base import BaseFileHelper
from src.file_helper.factory import FileHelperFactory
from src.settings import config
from src.sources.base import DataSource
from src.utils import get_file_name

logger = logging.getLogger(__name__)


class BaseReader(ABC):
    def __init__(self, file_path: Union[Path, str], source: DataSource, log_id: int):
        self.file_path: Union[Path, str] = file_path
        self.source: DataSource = source
        self.log_id: int = log_id
        self.source_filename: str = get_file_name(file_path)
        self.batch_size: int = config.BATCH_SIZE
        self.rows_read: int = 0
        self.file_helper: BaseFileHelper = FileHelperFactory.create_file_helper()
        self.is_gzipped: bool = self.file_helper.is_gzipped(file_path)

    @contextmanager
    def _get_file_stream(self, mode: str = "rb"):
        with self.file_helper.get_file_stream(self.file_path, mode) as stream:
            try:
                if self.is_gzipped:
                    yield gzip.open(stream, mode)
                else:
                    yield stream
            finally:
                if hasattr(stream, "close") and not getattr(stream, "_closed", True):
                    stream.close()

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
                    "required_fields_display_formatted": ", ".join(
                        required_fields_display
                    ),
                    "missing_fields_display_formatted": ", ".join(
                        missing_fields_display
                    ),
                    "archive_directory": str(config.ARCHIVE_PATH),
                }
            )

    @abstractmethod
    def read(self) -> Iterator[list[Dict[str, Any]]]:
        pass

    @property
    @abstractmethod
    def starting_row_number(self) -> int:
        pass

    def __iter__(self):
        return self.read()
