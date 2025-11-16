import logging
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

import pendulum
from opentelemetry import trace
from sqlalchemy import Engine, MetaData, Table, update
from sqlalchemy.orm import Session, sessionmaker

from src.exceptions import FILE_ERROR_EXCEPTIONS, DuplicateFileError
from src.notify.factory import NotifierFactory
from src.pipeline.db_utils import (
    db_check_if_duplicate_file,
    db_create_stage_table,
    db_start_log,
)
from src.pipeline.read.base import BaseReader
from src.pipeline.read.factory import ReaderFactory
from src.pipeline.validate.validator import Validator
from src.pipeline.write.base import BaseWriter
from src.pipeline.write.factory import WriterFactory
from src.process.file_helper import FileHelper
from src.process.log import FileLoadLog
from src.sources.base import DataSource
from src.utils import retry

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class PipelineRunner:
    def __init__(
        self,
        file_path: Path,
        source: DataSource,
        engine: Engine,
        metadata: MetaData,
        file_load_log_table: Table,
        file_load_dlq_table: Table,
    ):
        self.data_source: DataSource = source
        self.file_path: Path = file_path
        self.source_filename: str = file_path.name
        self.metadata: MetaData = metadata
        self.engine: Engine = engine
        self.Session: sessionmaker[Session] = sessionmaker(bind=self.engine)
        self.stage_table_name: Optional[str] = None
        self.file_load_log_table: Table = file_load_log_table
        self.file_load_dlq_table: Table = file_load_dlq_table
        self.reader: BaseReader = ReaderFactory.create_reader(file_path, source)
        self.writer: BaseWriter = WriterFactory.create_writer(
            source, engine, metadata, file_load_dlq_table
        )
        self.validator: Validator = Validator(
            file_path, source, self.reader.starting_row_number
        )
        self.result: Optional[tuple[bool, str]] = None
        self.log = FileLoadLog(
            source_filename=self.source_filename,
            started_at=pendulum.now("UTC"),
        )
        self.log.id: int = db_start_log(
            self.Session,
            self.file_load_log_table,
            self.log.source_filename,
            self.log.started_at,
        )

    @retry()
    def _log_update(self, log: FileLoadLog) -> None:
        vals = log.model_dump(
            exclude_unset=True, exclude={"id", "source_filename", "started_at"}
        )
        stmt = (
            update(self.file_load_log_table)
            .where(self.file_load_log_table.c.id == log.id)
            .values(**vals)
        )
        with self.Session() as session:
            session.execute(stmt)

    def check_if_processed(self) -> bool:
        already_processed = db_check_if_duplicate_file(
            self.Session, self.data_source, self.source_filename
        )
        if already_processed:
            logger.warning(
                f"[log_id: {self.log.id}] File {self.source_filename} has already been processed"
            )
            FileHelper.copy_file_to_duplicate_files(self.file_path)
            self.log.duplicate_skipped = True
            if self.reader.source.notification_emails:
                notifier = NotifierFactory.get_notifier("email")
                email_notifier = notifier(
                    source_filename=self.source_filename,
                    exception=DuplicateFileError,
                    recipient_emails=self.reader.source.notification_emails,
                )
                email_notifier.notify()
            self._log_update(self.log)
            raise DuplicateFileError(
                f"File {self.source_filename} has already been processed"
            )
        return already_processed

    def archive_file(self) -> None:
        self.log.archive_copy_started_at = pendulum.now("UTC")
        FileHelper.copy_file_to_archive(self.file_path)
        self.log.archive_copy_ended_at = pendulum.now("UTC")
        self.log.archive_copy_success = True
        self._log_update(self.log)

    def read_data(self) -> Iterator[list[Dict[str, Any]]]:
        self.log.read_started_at = pendulum.now("UTC")
        yield from self.reader.read()
        self.log.read_ended_at = pendulum.now("UTC")
        self.log.records_read = self.reader.rows_read
        self.log.read_success = True
        self._log_update(self.log)

    def validate_data(
        self, batches: Iterator[list[Dict[str, Any]]]
    ) -> Iterator[list[Dict[str, Any]]]:
        self.log.validate_started_at = pendulum.now("UTC")
        yield from self.validator.validate(batches)
        self.log.validate_ended_at = pendulum.now("UTC")
        self.log.validation_errors = self.validator.validation_errors
        self.log.validate_success = True
        self._log_update(self.log)

    def write_data(self, batches: Iterator[tuple[bool, list[Dict[str, Any]]]]) -> None:
        self.log.write_started_at = pendulum.now("UTC")
        self.stage_table_name = db_create_stage_table(
            self.engine, self.metadata, self.data_source, self.source_filename
        )
        self.writer.write(batches, self.stage_table_name)

        self.log.write_ended_at = pendulum.now("UTC")
        self.log.records_written_to_stage = self.writer.rows_written_to_stage
        self.log.write_success = True
        self._log_update(self.log)

    def audit_data(self):
        pass

    def publish_data(self):
        pass

    def cleanup(self) -> None:
        pass

    def run(self):
        with tracer.start_as_current_span(f"FILE: {self.source_filename}") as span:
            try:
                already_processed = self.check_if_processed()
                if not already_processed:
                    self.archive_file()
                    self.write_data(self.validate_data(self.read_data()))
                    self.audit_data()
                    self.publish_data()
                    self.cleanup()
                self.result = (True, self.source_filename)
            except Exception in FILE_ERROR_EXCEPTIONS:
                self.log.error_type = e.error_type
                self._log_update(self.log)
            except Exception as e:
                logger.exception(
                    f"Error running pipeline for file {self.source_filename}: {e}"
                )
                self.result = (False, self.source_filename)
            return self.result
