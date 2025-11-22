import logging
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
from queue import Empty, Queue
from typing import Optional

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


class Processor:
    def __init__(self):
        self.thread_pool: ThreadPoolExecutor = ThreadPoolExecutor(
            max_workers=multiprocessing.cpu_count()
        )
        self.file_helper: BaseFileHelper = FileHelperFactory.create_file_helper()
        self.file_paths_queue: Queue = self.file_helper.scan_directory(
            config.DIRECTORY_PATH
        )
        self.engine, self.metadata = setup_db()
        create_tables(self.metadata, self.engine)
        self.file_load_log_table: Table = self.metadata.tables["file_load_log"]
        self.file_load_dlq_table: Table = self.metadata.tables["file_load_dlq"]
        self.results: list[tuple[bool, str, Optional[str]]] = []

    def process_file(self, file_name: str):
        file_path = self.file_helper.get_file_path(config.DIRECTORY_PATH, file_name)
        try:
            source = MASTER_REGISTRY.find_source_for_file(file_path)
        except Exception as e:
            logger.exception(f"Error finding source for file {file_name}: {e}")
            self.results.append((False, file_name, str(e)))
        if source is not None:
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
                (False, file_name, f"No source found for file {file_name}")
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
        files_failed = {}
        for result in self.results:
            if result[0]:
                success_count += 1
            else:
                failure_count += 1
                files_failed[result[1]] = result[2]
        logger.info(
            f"Processing complete with {success_count} success(es) and {failure_count} failure(s)"
        )
        if files_failed:
            failure_details = "\n".join(
                f"• {filename}: {error_message}"
                for filename, error_message in files_failed.items()
            )
            message = f"Some files failed to process:\n{failure_details}"
            notifier = NotifierFactory.get_notifier("slack")
            slack_notifier = notifier(
                level=AlertLevel.ERROR,
                title="File Processing Failure Summary",
                message=message,
            )
            slack_notifier.notify()

    def __del__(self):
        if hasattr(self, "thread_pool"):
            self.thread_pool.shutdown(wait=True)
        if hasattr(self, "engine"):
            self.engine.dispose()
