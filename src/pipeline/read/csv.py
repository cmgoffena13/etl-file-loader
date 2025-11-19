import csv
import gzip
from pathlib import Path
from typing import Any, Dict, Iterator

from src.exception.exceptions import MissingHeaderError
from src.pipeline.read.base import BaseReader
from src.settings import config
from src.sources.base import CSVSource


class CSVReader(BaseReader):
    SOURCE_TYPE = CSVSource

    def __init__(
        self, file_path: Path, source, delimiter: str, encoding: str, skip_rows: int
    ):
        super().__init__(file_path, source)
        self.delimiter: str = delimiter
        self.encoding: str = encoding
        self.skip_rows: int = skip_rows

    @property
    def starting_row_number(self) -> int:
        """CSV: Row 1 = header, so starting row = 2 + skip_rows."""
        return 2 + self.skip_rows

    def read(self) -> Iterator[list[Dict[str, Any]]]:
        file_opener = gzip.open if self.is_gzipped else open
        file_mode = "rt" if self.is_gzipped else "r"

        with file_opener(
            self.file_path, file_mode, encoding=self.encoding, newline=""
        ) as csvfile:
            reader = csv.DictReader(csvfile, delimiter=self.delimiter)

            if not reader.fieldnames:
                raise MissingHeaderError(
                    f"No header found in CSV file: {self.file_path}"
                )

            if not any(
                fieldname and fieldname.strip() for fieldname in reader.fieldnames
            ):
                raise MissingHeaderError(
                    f"Whitespace-only header found in CSV file: {self.file_path}"
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
                    yield batch
                    batch[:] = [None] * self.batch_size
                    batch_index = 0

            if batch_index > 0:
                yield batch[:batch_index]
