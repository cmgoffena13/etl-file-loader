import logging
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

from opentelemetry import trace
from sqlalchemy import Engine, MetaData, Table
from sqlalchemy.orm import Session, sessionmaker

from src.exceptions import DuplicateFileError
from src.notify.factory import NotifierFactory
from src.pipeline.db_utils import db_check_if_duplicate_file, db_create_stage_table
from src.pipeline.read.base import BaseReader
from src.pipeline.read.factory import ReaderFactory
from src.pipeline.validate.validator import Validator
from src.pipeline.write.base import BaseWriter
from src.pipeline.write.factory import WriterFactory
from src.process.file_helper import FileHelper
from src.sources.base import DataSource

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

    def check_if_processed(self) -> None:
        if db_check_if_duplicate_file(
            self.Session, self.data_source, self.source_filename
        ):
            logger.warning(f"File {self.source_filename} has already been processed")
            FileHelper.copy_file_to_duplicate_files(self.file_path)
            if self.reader.source.notification_emails:
                notifier = NotifierFactory.get_notifier("email")
                email_notifier = notifier(
                    source_filename=self.source_filename,
                    exception=DuplicateFileError,
                    recipient_emails=self.reader.source.notification_emails,
                )
                email_notifier.notify()

    def read_data(self) -> Iterator[list[Dict[str, Any]]]:
        yield from self.reader.read()

    def validate_data(
        self, batches: Iterator[list[Dict[str, Any]]]
    ) -> Iterator[list[Dict[str, Any]]]:
        yield from self.validator.validate(batches)

    def write_data(self, batches: Iterator[tuple[bool, list[Dict[str, Any]]]]) -> None:
        self.stage_table_name = db_create_stage_table(
            self.engine, self.metadata, self.data_source, self.source_filename
        )
        self.writer.write(batches, self.stage_table_name)

    def audit_data(self):
        pass

    def publish_data(self):
        pass

    def cleanup(self) -> None:
        pass

    def run(self):
        with tracer.start_as_current_span(f"FILE: {self.source_filename}") as span:
            try:
                self.check_if_processed()
                self.write_data(self.validate_data(self.read_data()))
                self.audit_data()
                self.publish_data()
                self.cleanup()
                self.result = (True, self.source_filename)
            except Exception as e:
                logger.exception(
                    f"Error running pipeline for file {self.source_filename}: {e}"
                )
                self.result = (False, self.source_filename)
            return self.result
