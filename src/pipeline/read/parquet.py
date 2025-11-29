import logging
from pathlib import Path
from typing import Any, Dict, Iterator, Union

import pyarrow.parquet as pq

from src.exception.exceptions import MissingHeaderError, NoDataInFileError
from src.pipeline.read.base import BaseReader
from src.settings import config
from src.sources.base import DataSource, ParquetSource

logger = logging.getLogger(__name__)


class ParquetReader(BaseReader):
    SOURCE_TYPE = ParquetSource

    def __init__(
        self,
        file_path: Union[Path, str],
        source: DataSource,
        log_id: int,
    ):
        super().__init__(file_path, source, log_id)

    @property
    def starting_row_number(self) -> int:
        """Parquet: No header row, so starting row = 1"""
        return 1

    def _read_from_parquet_file(
        self, parquet_file: pq.ParquetFile
    ) -> Iterator[list[Dict[str, Any]]]:
        """Read row groups from a ParquetFile object."""
        if parquet_file.metadata.num_rows == 0:
            logger.error(f"No data found in Parquet file: {self.source_filename}")
            raise NoDataInFileError(
                error_values={"archive_directory": str(config.ARCHIVE_PATH)}
            )

        schema = parquet_file.schema_arrow
        actual_fields = set(schema.names)

        # Parquet files always have a schema, but validate it has column names
        if not actual_fields or not any(field.strip() for field in actual_fields):
            logger.error(
                f"No column names found in Parquet file schema: {self.source_filename}"
            )
            raise MissingHeaderError(
                error_values={"archive_directory": str(config.ARCHIVE_PATH)}
            )

        self._validate_fields(actual_fields)

        batch = [None] * self.batch_size
        batch_index = 0
        logger.info(f"[log_id={self.log_id}] Reading file: {self.source_filename}")
        for record_batch in parquet_file.iter_batches(batch_size=self.batch_size):
            for i in range(record_batch.num_rows):
                row_dict = {
                    col: record_batch[col][i].as_py()
                    for col in record_batch.schema.names
                }
                batch[batch_index] = row_dict
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

    def read(self) -> Iterator[list[Dict[str, Any]]]:
        if isinstance(self.file_path, Path):
            parquet_file = pq.ParquetFile(self.file_path)
            yield from self._read_from_parquet_file(parquet_file)
            return

        with self._get_file_stream("rb") as stream:
            parquet_file = pq.ParquetFile(stream)
            yield from self._read_from_parquet_file(parquet_file)
