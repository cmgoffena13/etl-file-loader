from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Union

import pendulum
import structlog
from sqlalchemy import Engine, MetaData, Table, update
from sqlalchemy.orm import Session, sessionmaker
from structlog.contextvars import bind_contextvars, clear_contextvars

from src.exception.base import BaseFileErrorEmailException
from src.exception.exceptions import (
    DuplicateFileError,
    ValidationThresholdExceededError,
)
from src.file_helper.base import BaseFileHelper
from src.notify.factory import NotifierFactory
from src.pipeline.audit.base import BaseAuditor
from src.pipeline.audit.factory import AuditorFactory
from src.pipeline.db_utils import (
    db_check_if_duplicate_file,
    db_create_stage_table,
    db_drop_stage_table,
    db_start_log,
)
from src.pipeline.delete.base import BaseDeleter
from src.pipeline.delete.factory import DeleterFactory
from src.pipeline.publish.base import BasePublisher
from src.pipeline.publish.factory import PublisherFactory
from src.pipeline.read.base import BaseReader
from src.pipeline.read.factory import ReaderFactory
from src.pipeline.validate.validator import Validator
from src.pipeline.write.base import BaseWriter
from src.pipeline.write.factory import WriterFactory
from src.process.log import FileLoadLog
from src.settings import config
from src.sources.base import DataSource
from src.utils import get_error_location, get_file_name, retry

logger = structlog.getLogger(__name__)


class PipelineRunner:
    def __init__(
        self,
        file_path: Union[Path, str],
        source: DataSource,
        engine: Engine,
        metadata: MetaData,
        file_load_log_table: Table,
        file_load_dlq_table: Table,
        file_helper: BaseFileHelper,
    ):
        clear_contextvars()
        self.source: DataSource = source
        self.file_path: Union[Path, str] = file_path
        self.source_filename: str = get_file_name(file_path)
        self.metadata: MetaData = metadata
        self.engine: Engine = engine
        self.Session: sessionmaker[Session] = sessionmaker(bind=self.engine)
        self.file_load_log_table: Table = file_load_log_table
        self.file_load_dlq_table: Table = file_load_dlq_table
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
        bind_contextvars(log_id=self.log.id, source_filename=self.source_filename)
        logger.info(f"Processing file: {self.source_filename}")
        self.file_helper: BaseFileHelper = file_helper
        self.stage_table_name: str = db_create_stage_table(
            self.engine, self.metadata, self.source, self.source_filename, self.log.id
        )
        self.reader: BaseReader = ReaderFactory.create_reader(
            self.file_path, self.source, self.log.id
        )
        self.validator: Validator = Validator(
            self.file_path, self.source, self.reader.starting_row_number, self.log.id
        )
        self.writer: BaseWriter = WriterFactory.create_writer(
            source=self.source,
            engine=self.engine,
            file_load_dlq_table=self.file_load_dlq_table,
            log_id=self.log.id,
            stage_table_name=self.stage_table_name,
        )
        self.auditor: BaseAuditor = AuditorFactory.create_auditor(
            file_path=self.file_path,
            source=self.source,
            engine=self.engine,
            stage_table_name=self.stage_table_name,
            log_id=self.log.id,
        )
        self.publisher: BasePublisher = PublisherFactory.create_publisher(
            source=self.source,
            engine=self.engine,
            log_id=self.log.id,
            stage_table_name=self.stage_table_name,
            rows_written_to_stage=self.writer.rows_written_to_stage,
        )
        self.deleter: BaseDeleter = DeleterFactory.create_deleter(
            source_filename=self.source_filename,
            engine=self.engine,
            log_id=self.log.id,
            file_load_dlq_table=self.file_load_dlq_table,
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
            session.commit()

    def check_if_processed(self) -> None:
        if db_check_if_duplicate_file(self.Session, self.source, self.source_filename):
            logger.warning(f"File {self.source_filename} has already been processed")
            self.file_helper.copy_file_to_duplicate_files(self.file_path)
            self.log.duplicate_skipped = True
            self._log_update(self.log)
            raise DuplicateFileError(
                error_values={
                    "duplicate_directory": str(config.DUPLICATE_FILES_PATH),
                }
            )

        self.log.duplicate_skipped = False
        self._log_update(self.log)

    def archive_file(self) -> None:
        self.log.archive_copy_started_at = pendulum.now("UTC")

        self.file_helper.copy_file_to_archive(self.file_path)

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

        self.log.validation_errors = self.validator.validation_errors
        self.log.validate_ended_at = pendulum.now("UTC")
        self.log.validate_success = True
        self._log_update(self.log)

    def write_data(self, batches: Iterator[tuple[bool, list[Dict[str, Any]]]]) -> None:
        self.log.write_started_at = pendulum.now("UTC")

        self.writer.write(batches)

        # update publisher with the actual number of rows written to stage
        self.publisher.rows_written_to_stage = self.writer.rows_written_to_stage

        self.log.write_ended_at = pendulum.now("UTC")
        self.log.records_written_to_stage = self.writer.rows_written_to_stage
        self.log.write_success = True
        self._log_update(self.log)

    def audit_data(self) -> None:
        self.log.audit_started_at = pendulum.now("UTC")

        self.auditor.audit_grain()
        self.auditor.audit_data()

        self.log.audit_ended_at = pendulum.now("UTC")
        self.log.audit_success = True
        self._log_update(self.log)

    def publish_data(self) -> None:
        self.log.publish_started_at = pendulum.now("UTC")

        self.publisher.publish()

        self.log.publish_ended_at = pendulum.now("UTC")
        self.log.publish_success = True
        self.log.publish_inserts = self.publisher.publish_inserts
        self.log.publish_updates = self.publisher.publish_updates
        self._log_update(self.log)

    def cleanup_dlq_records(self) -> None:
        self.deleter.delete()

    def cleanup(self) -> None:
        db_drop_stage_table(self.stage_table_name, self.Session, self.log.id)

    def run(self) -> tuple[bool, str, Optional[str]]:
        try:
            self.check_if_processed()
            self.archive_file()
            self.write_data(self.validate_data(self.read_data()))
            self.audit_data()
            self.publish_data()
            self.cleanup_dlq_records()
            self.cleanup()
            self.log.ended_at = pendulum.now("UTC")
            self.log.success = True
            self.result = (True, self.source_filename, None)
            duration = (self.log.ended_at - self.log.started_at).total_seconds()
            logger.info(
                f"Pipeline completed successfully for file:  {self.source_filename} - took {duration:.2f} seconds"
            )
        except Exception as e:
            self.log.success = None if isinstance(e, DuplicateFileError) else False
            self.log.error_type = type(e).__name__
            if isinstance(e, BaseFileErrorEmailException):
                if isinstance(e, ValidationThresholdExceededError):
                    self.log.validation_errors = self.validator.validation_errors
                if self.reader.source.notification_emails:
                    error_values = getattr(e, "error_values", {})
                    notifier = NotifierFactory.get_notifier("email")
                    email_notifier = notifier(
                        source_filename=self.source_filename,
                        log_id=self.log.id,
                        exception=e,
                        recipient_emails=self.reader.source.notification_emails,
                        **error_values,
                    )
                    email_notifier.notify()
                    # Success since email notification was sent
                    self.result = (
                        True,
                        self.source_filename,
                        None,
                    )
                # Failure since email notification was not sent, notifies through webhook instead
                if self.result is None:
                    self.result = (
                        False,
                        self.source_filename,
                        type(e).__name__,
                    )
            else:
                error_location = get_error_location(e)
                logger.exception(
                    f"Error running pipeline for file {self.source_filename}: {e} at {error_location}"
                )
                self.result = (
                    False,
                    self.source_filename,
                    f"{str(e)} at {error_location}",
                )
        finally:
            self.file_helper.delete_file(self.file_path)
            self._log_update(self.log)
        return self.result

    def __del__(self):
        pass
