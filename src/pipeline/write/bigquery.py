import base64
import logging
import time
from collections.abc import Iterator
from decimal import Decimal
from typing import Any, Dict
from urllib.parse import urlparse

from google.cloud import bigquery
from pydantic_extra_types.pendulum_dt import Date, DateTime
from sqlalchemy import Engine, Table, insert

from src.pipeline.write.base import BaseWriter
from src.settings import config
from src.sources.base import DataSource

logger = logging.getLogger(__name__)


class BigQueryWriter(BaseWriter):
    def __init__(
        self,
        source: DataSource,
        engine: Engine,
        file_load_dlq_table: Table,
        log_id: int,
        stage_table_name: str,
    ):
        super().__init__(source, engine, file_load_dlq_table, log_id, stage_table_name)

        # Parse dataset from DATABASE_URL (format: bigquery://project_id/dataset_id)
        parsed = urlparse(config.DATABASE_URL)
        self._project_id = parsed.netloc
        self._dataset_id = parsed.path.strip("/")

        if not self._project_id or not self._dataset_id:
            raise ValueError(
                f"Invalid BigQuery DATABASE_URL format: {config.DATABASE_URL}"
            )

        self._client = bigquery.Client(project=self._project_id)

    def _convert_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Convert record for BigQuery: handle float to Decimal conversion, bytes to base64, and Pendulum dates."""
        converted = {}
        for key, value in record.items():
            if isinstance(value, float):
                converted[key] = str(Decimal(str(value)))
            elif isinstance(value, Decimal):
                converted[key] = str(value)
            elif isinstance(value, (Date, DateTime)):
                converted[key] = value.isoformat()
            elif isinstance(value, bytes):
                converted[key] = base64.b64encode(value).decode("utf-8")
            else:
                converted[key] = value
        return converted

    def _load_batch(self, records: list[Dict[str, Any]]) -> int:
        table_id = f"{self._project_id}.{self._dataset_id}.{self.stage_table_name}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        job = self._client.load_table_from_json(
            records, table_id, job_config=job_config
        )
        job.result()

        if job.errors:
            raise RuntimeError(f"BigQuery load job failed: {job.errors}")

        return len(records)

    def write(self, batches: Iterator[tuple[bool, list[Dict[str, Any]]]]) -> None:
        """Override to use BigQuery bulk loading via load_table_from_json."""
        logger.info(
            f"[log_id={self.log_id}] Writing data to stage table: {self.stage_table_name} using bulk load"
        )

        valid_records = [None] * self.batch_size
        valid_index = 0
        invalid_records = []
        logger.info(
            f"[log_id={self.log_id}] Writing data to stage table: {self.stage_table_name} using bulk load"
        )
        for batch in batches:
            for passed, record in batch:
                record = self._convert_record(record)
                if passed:
                    valid_records[valid_index] = record
                    valid_index += 1

                    if valid_index >= self.batch_size:
                        rows_loaded = self._load_batch(valid_records[:valid_index])
                        self.rows_written_to_stage += rows_loaded
                        logger.debug(
                            f"[log_id={self.log_id}] Loaded {rows_loaded} rows to {self.stage_table_name}"
                        )
                        valid_records[:] = [None] * self.batch_size
                        valid_index = 0
                else:
                    if "id" not in record:
                        record["id"] = int(time.time_ns() // 1000) + len(
                            invalid_records
                        )
                    invalid_records.append(record)
                    if len(invalid_records) >= self.batch_size:
                        with self.Session() as session:
                            try:
                                stmt = insert(self.file_load_dlq_table).values(
                                    invalid_records
                                )
                                session.execute(stmt)
                                session.commit()
                                invalid_records.clear()
                            except Exception as e:
                                logger.exception(
                                    f"Error inserting records into file load DLQ table: {e}"
                                )
                                session.rollback()
                                raise e
            if (
                self.rows_written_to_stage % 100000 == 0
                or self.rows_written_to_stage < 100000
            ) and self.rows_written_to_stage > 0:
                logger.info(
                    f"[log_id={self.log_id}] Rows written: {self.rows_written_to_stage}"
                )
        if valid_index > 0:
            logger.debug(
                f"[log_id={self.log_id}] Writing final batch of {valid_index} rows to stage table: {self.stage_table_name}"
            )
            rows_loaded = self._load_batch(valid_records[:valid_index])
            self.rows_written_to_stage += rows_loaded
        if invalid_records:
            with self.Session() as session:
                try:
                    logger.debug(
                        f"[log_id={self.log_id}] Writing final batch of {len(invalid_records)} rows to dlq table: {self.file_load_dlq_table_name}"
                    )
                    stmt = insert(self.file_load_dlq_table).values(invalid_records)
                    session.execute(stmt)
                    session.commit()
                except Exception as e:
                    logger.exception(
                        f"Error inserting records into file load DLQ table: {e}"
                    )
                    session.rollback()
                    raise e
