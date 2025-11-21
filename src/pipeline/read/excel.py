import logging
from itertools import chain
from pathlib import Path
from typing import Any, Dict, Iterator, get_args, get_origin

import pendulum
import pyexcel
from pydantic_extra_types.pendulum_dt import Date, DateTime

from src.exception.exceptions import MissingHeaderError
from src.pipeline.read.base import BaseReader
from src.sources.base import DataSource, ExcelSource

logger = logging.getLogger(__name__)


class ExcelReader(BaseReader):
    # Excel epoch: 1899-12-30 (Excel's epoch with 1900 leap year bug)
    # Serial number 1 = 1900-01-01
    _EXCEL_EPOCH = pendulum.datetime(1899, 12, 30)

    SOURCE_TYPE = ExcelSource

    def __init__(
        self,
        file_path: Path,
        source: DataSource,
        log_id: int,
        sheet_name: str,
        skip_rows: int,
    ):
        super().__init__(file_path, source, log_id)
        self.sheet_name: str = sheet_name
        self.skip_rows: int = skip_rows

    @property
    def starting_row_number(self) -> int:
        """Excel: Row 1 = header (name_columns_by_row=0), so starting row = 2 + skip_rows."""
        return 2 + self.skip_rows

    def _build_date_field_mapping(self) -> Dict[str, type]:
        date_field_mapping = {}
        for name, field in self.source.source_model.model_fields.items():
            field_type = field.annotation
            # Unwrap Optional[T] (which is Union[T, None]) to get T
            origin = get_origin(field_type)
            if origin is not None:
                args = get_args(field_type)
                field_type = args[0] if args else field_type
            else:
                field_type = field.annotation

            if field_type in (Date, DateTime):
                date_field_mapping[name.lower()] = field_type
                if field.alias:
                    date_field_mapping[field.alias.lower()] = field_type
        return date_field_mapping

    def _convert_excel_dates(
        self, record: Dict[str, Any], date_field_mapping: Dict[str, type]
    ) -> Dict[str, Any]:
        converted_record = {}
        for key, value in record.items():
            key_lower = key.lower()
            if key_lower in date_field_mapping and isinstance(value, (int, float)):
                days = int(value)
                fractional = value - days

                dt = self._EXCEL_EPOCH.add(days=days)

                if fractional > 0:
                    seconds = int(fractional * 86400)
                    dt = dt.add(seconds=seconds)

                if date_field_mapping[key_lower] == Date:
                    converted_record[key] = dt.date()
                else:
                    converted_record[key] = dt
            else:
                converted_record[key] = value
        return converted_record

    def read(self) -> Iterator[list[Dict[str, Any]]]:
        records = pyexcel.iget_records(
            file_name=str(self.file_path),
            sheet_name=self.sheet_name,
            name_columns_by_row=0,
        )

        try:
            first_record = next(records)
        except StopIteration:
            raise ValueError(f"No data found in Excel file: {self.file_path}")

        actual_headers = set(first_record.keys())

        no_valid_headers = not any(
            isinstance(key, str) and key.strip() for key in actual_headers
        )
        all_default_names = len(actual_headers) > 0 and all(
            (
                not key
                or not str(key).strip()
                or (isinstance(key, str) and str(key).strip().lstrip("-").isdigit())
            )
            for key in actual_headers
        )

        if no_valid_headers or all_default_names:
            raise MissingHeaderError(
                error_values={"source_filename": self.file_path.name}
            )

        self._validate_fields(actual_headers)

        date_field_mapping = self._build_date_field_mapping()

        # Merge first record back into the iterator
        all_records = chain([first_record], records)

        batch = [None] * self.batch_size
        batch_index = 0
        for index, record in enumerate(all_records, start=1):
            if index <= self.skip_rows:
                continue

            batch[batch_index] = self._convert_excel_dates(record, date_field_mapping)
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
