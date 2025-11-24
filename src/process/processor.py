import logging
from concurrent.futures import ThreadPoolExecutor
from queue import Empty, Queue
from typing import Optional

import psutil
from opentelemetry import trace
from sqlalchemy import Table

from src.file_helper.base import BaseFileHelper
from src.file_helper.factory import FileHelperFactory
from src.notify.factory import NotifierFactory
from src.notify.slack import AlertLevel
from src.pipeline.runner import PipelineRunner
from src.process.db import create_tables, setup_db
from src.settings import config
from src.sources.master import MASTER_REGISTRY

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class Processor:
    def __init__(
        self,
        directory_path: Optional[str] = None,
        archive_path: Optional[str] = None,
        duplicate_files_path: Optional[str] = None,
    ):
        if directory_path:
            config.DIRECTORY_PATH = directory_path
        if archive_path:
            config.ARCHIVE_PATH = archive_path
        if duplicate_files_path:
            config.DUPLICATE_FILES_PATH = duplicate_files_path
        if (
            directory_path
            or archive_path
            or duplicate_files_path
            and not all([directory_path, archive_path, duplicate_files_path])
        ):
            logger.error(
                "Directory path, archive path, and duplicate files path are required if any one is provided"
            )
            raise ValueError(
                "Directory path, archive path, and duplicate files path are required if any one is provided"
            )
        logger.info("Processor Initialized")
        self.thread_pool: ThreadPoolExecutor = ThreadPoolExecutor(
            max_workers=psutil.cpu_count(logical=False)
        )
        self.file_helper: BaseFileHelper = FileHelperFactory.create_file_helper()
        self.file_paths_queue: Queue = self.file_helper.scan_directory(
            config.DIRECTORY_PATH
        )
        self.engine, self.metadata = setup_db()
        create_tables(self.metadata, self.engine)
        self.file_load_log_table: Table = self.metadata.tables["file_load_log"]
        self.file_load_dlq_table: Table = self.metadata.tables["file_load_dlq"]
        self.results: list[tuple[Optional[bool], str, Optional[str]]] = []

    def process_file(self, file_name: str):
        file_path = self.file_helper.get_file_path(config.DIRECTORY_PATH, file_name)
        try:
            source = MASTER_REGISTRY.find_source_for_file(file_path)
        except Exception as e:
            logger.exception(f"Error finding source for file {file_name}: {e}")
            self.results.append((False, file_name, str(e)))
        if source is not None:
            with tracer.start_as_current_span(f"File: {file_name}"):
                runner = PipelineRunner(
                    file_path=file_path,
                    source=source,
                    engine=self.engine,
                    metadata=self.metadata,
                    file_load_log_table=self.file_load_log_table,
                    file_load_dlq_table=self.file_load_dlq_table,
                    file_helper=self.file_helper,
                )
                result = runner.run()
                self.results.append(result)
        else:
            self.file_helper.copy_file_to_archive(file_path)
            self.results.append(
                (None, file_name, f"No source found for file: {file_name}")
            )

    def _worker(self, file_paths_queue: Queue):
        while True:
            try:
                file_name = file_paths_queue.get_nowait()
                self.process_file(file_name)
                file_paths_queue.task_done()
            except Empty:
                break

    def process_files_in_parallel(self):
        logger.info(
            f"Processing {self.file_paths_queue.qsize()} files in parallel with {self.thread_pool._max_workers} workers"
        )
        try:
            futures = [
                self.thread_pool.submit(self._worker, self.file_paths_queue)
                for _ in range(self.thread_pool._max_workers)
            ]
            for future in futures:
                future.result()
            self.results_summary()
        finally:
            self.thread_pool.shutdown(wait=True)

    def results_summary(self):
        success_count = 0
        failure_count = 0
        no_source_count = 0
        files_failed = {}
        files_no_source = {}
        for result in self.results:
            status, filename, error_message = result
            if status is True:
                success_count += 1
            elif status is False:
                failure_count += 1
                files_failed[filename] = error_message
            else:
                no_source_count += 1
                files_no_source[filename] = error_message

        logger.info(
            f"Processing complete: {success_count} successful, {failure_count} failed, {no_source_count} no source found"
        )

        if files_failed or files_no_source:
            details = []
            if files_failed:
                failure_details = "\n".join(
                    f"• {filename}: {error_message}"
                    for filename, error_message in files_failed.items()
                )
                details.append(f"\n\nFailed:\n{failure_details}")
            if files_no_source:
                no_source_details = "\n".join(
                    f"• {filename}: {error_message}"
                    for filename, error_message in files_no_source.items()
                )
                details.append(f"\n\nNo Source Found:\n{no_source_details}")

            message = "\n\n".join(details)
            notifier = NotifierFactory.get_notifier("slack")
            slack_notifier = notifier(
                level=AlertLevel.ERROR,
                title="File Processing Summary",
                message=message,
            )
            slack_notifier.notify()

    def __del__(self):
        if hasattr(self, "thread_pool"):
            self.thread_pool.shutdown(wait=True)
        if hasattr(self, "engine"):
            self.engine.dispose()
