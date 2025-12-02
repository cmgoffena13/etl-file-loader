from decimal import Decimal
from itertools import chain
from pathlib import Path
from typing import Any, Dict, Iterator, Union

import ijson
import structlog

from src.exception.exceptions import NoDataInFileError
from src.pipeline.read.base import BaseReader
from src.settings import config
from src.sources.base import DataSource, JSONSource

logger = structlog.getLogger(__name__)


class JSONReader(BaseReader):
    SOURCE_TYPE = JSONSource

    def __init__(
        self,
        file_path: Union[Path, str],
        source: DataSource,
        log_id: int,
        array_path: str,
    ):
        super().__init__(file_path, source, log_id)
        self.array_path: str = array_path

    @property
    def starting_row_number(self) -> int:
        """JSON: No header, so starting row = 1"""
        return 1

    def _convert_decimals_to_float(self, value: Any) -> Any:
        """Convert Decimal values to float for database compatibility."""
        if isinstance(value, Decimal):
            return float(value)
        return value

    def _flatten_dict(
        self, dictionary: Dict[str, Any], parent_key: str = "", sep: str = "_"
    ) -> Dict[str, Any]:
        items = []
        for key, value in dictionary.items():
            new_key = f"{parent_key}{sep}{key}".lower() if parent_key else key.lower()
            if isinstance(value, dict):
                items.extend(self._flatten_dict(value, new_key, sep=sep).items())
            elif isinstance(value, list):
                # Handle lists by converting to string or flattening if they contain dicts
                if value and isinstance(value[0], dict):
                    # If list contains dicts, flatten each dict with index
                    for index, item in enumerate(value):
                        if isinstance(item, dict):
                            items.extend(
                                self._flatten_dict(
                                    item, f"{new_key}{sep}{index}".lower(), sep=sep
                                ).items()
                            )
                        else:
                            items.append(
                                (
                                    f"{new_key}{sep}{index}".lower(),
                                    self._convert_decimals_to_float(item),
                                )
                            )
                else:
                    items.append((new_key, str(self._convert_decimals_to_float(value))))
            else:
                items.append((new_key, self._convert_decimals_to_float(value)))
        return dict(items)

    def read(self) -> Iterator[list[Dict[str, Any]]]:
        """Read JSON file iteratively in batches.

        Note: JSON keys must match the Pydantic model field names or aliases.
        Flattening preserves JSON key structure (e.g., nested {"Entry": {"ID": 1}}
        becomes "Entry_ID"), so JSON structure should align with model expectations.
        """
        with self._get_file_stream("rb") as file:
            objects = ijson.items(file, self.array_path)

            try:
                first_obj = next(objects)
            except StopIteration:
                raise NoDataInFileError(
                    error_values={"archive_directory": str(config.ARCHIVE_PATH)}
                )

            # Validate fields using first object
            if isinstance(first_obj, list) and first_obj:
                first_item = first_obj[0]
            elif isinstance(first_obj, list):
                logger.error(f"No data found in JSON file: {self.file_path}")
                raise NoDataInFileError(
                    error_values={"archive_directory": str(config.ARCHIVE_PATH)}
                )
            else:
                first_item = first_obj

            flattened_first = self._flatten_dict(first_item)
            actual_fields = set(flattened_first.keys())
            self._validate_fields(actual_fields)

            # Merge first object back into the iterator
            all_objects = chain([first_obj], objects)

            batch = [None] * self.batch_size
            batch_index = 0
            logger.info(f"Reading file: {self.source_filename}")
            for object in all_objects:
                items_to_process = object if isinstance(object, list) else list(object)
                for item in items_to_process:
                    batch[batch_index] = self._flatten_dict(item)
                    batch_index += 1
                    self.rows_read += 1

                    if batch_index == self.batch_size:
                        logger.debug(f"Reading batch of {self.batch_size} rows")
                        yield batch
                        batch[:] = [None] * self.batch_size
                        batch_index = 0

            if batch_index > 0:
                logger.debug(f"Reading final batch of {batch_index} rows")
                yield batch[:batch_index]
