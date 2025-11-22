import csv
import gzip
import io
import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, Union

from src.exception.exceptions import MissingHeaderError
from src.pipeline.read.base import BaseReader
from src.sources.base import CSVSource, DataSource

logger = logging.getLogger(__name__)


class CSVReader(BaseReader):
    SOURCE_TYPE = CSVSource

    def __init__(
        self,
        file_path: Union[Path, str],
        source: DataSource,
        log_id: int,
        delimiter: str,
        encoding: str,
        skip_rows: int,
    ):
        super().__init__(file_path, source, log_id)
        self.delimiter: str = delimiter
        self.encoding: str = encoding
        self.skip_rows: int = skip_rows

    @property
    def starting_row_number(self) -> int:
        """CSV: Row 1 = header, so starting row = 2 + skip_rows."""
        return 2 + self.skip_rows

    @contextmanager
    def _get_file_stream(self, mode: str = "rb"):
        """Get file stream context manager for CSV reading. Converts binary to text."""
        with super()._get_file_stream("rb") as stream:
            if self.is_gzipped:
                yield gzip.open(stream, "rt", encoding=self.encoding)
            else:
                yield io.TextIOWrapper(stream, encoding=self.encoding, newline="")

    def read(self) -> Iterator[list[Dict[str, Any]]]:
        with self._get_file_stream() as csvfile:
            reader = csv.DictReader(csvfile, delimiter=self.delimiter)

            if not reader.fieldnames:
                logger.error(f"No header found in file: {self.source_filename}")
                raise MissingHeaderError(
                    error_values={"source_filename": self.source_filename}
                )

            if not any(
                fieldname and fieldname.strip() for fieldname in reader.fieldnames
            ):
                logger.error(f"No header found in file: {self.source_filename}")
                raise MissingHeaderError(
                    error_values={"source_filename": self.source_filename}
                )

            self._validate_fields(set(reader.fieldnames))

            batch = [None] * self.batch_size
            batch_index = 0
            for index, row in enumerate(reader):
                if index < self.skip_rows:
                    continue

                batch[batch_index] = row
                batch_index += 1
                self.rows_read += 1

                if batch_index == self.batch_size:
                    logger.debug(
                        f"[log_id={self.log_id}] Reading batch of {self.batch_size} rows"
                    )
                    yield batch
                    batch[:] = [None] * self.batch_size
                    batch_index = 0

            if batch_index > 0:
                logger.debug(
                    f"[log_id={self.log_id}] Reading final batch of {batch_index} rows"
                )
                yield batch[:batch_index]
