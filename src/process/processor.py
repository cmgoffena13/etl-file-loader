import logging
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from queue import Empty, Queue

from sqlalchemy import Engine, MetaData, Table

from process.file_helper import FileHelper
from src.notify.factory import NotifierFactory
from src.notify.slack import AlertLevel
from src.pipeline.runner import PipelineRunner
from src.process.db import setup_db
from src.settings import config
from src.sources.master import MASTER_REGISTRY

engine, metadata = setup_db()

logger = logging.getLogger(__name__)


class Processor:
    def __init__(self):
        self.thread_pool: ThreadPoolExecutor = ThreadPoolExecutor(
            max_workers=multiprocessing.cpu_count()
        )
        self.file_paths_queue: Queue = FileHelper.scan_directory(config.DIRECTORY_PATH)
        self.engine: Engine = engine
        self.metadata: MetaData = metadata
        self.metadata.reflect(bind=self.engine, only=["file_load_log", "file_load_dlq"])
        self.file_load_log_table: Table = Table(
            "file_load_log", self.metadata, autoload_with=self.engine
        )
        self.file_load_dlq_table: Table = Table(
            "file_load_dlq", self.metadata, autoload_with=self.engine
        )
        self.results: list[tuple[bool, str]] = []

    def process_file(self, file_path: Path):
        try:
            source = MASTER_REGISTRY.find_source_for_file(file_path)
            if source is not None:
                runner = PipelineRunner(file_path, source, self.engine, self.metadata)
                result = runner.run()
                self.results.append(result)
        except Exception as e:
            logger.exception(f"Error processing file {file_path.name}: {e}")
            notifier = NotifierFactory.get_notifier("slack")
            slack_notifier = notifier(
                level=AlertLevel.ERROR,
                title=f"Error processing file {file_path.name}",
                message=str(e),
            )
            slack_notifier.notify()
            self.results.append((False, file_path.name))

    def _worker(self, file_paths_queue: Queue):
        while True:
            try:
                file_path = file_paths_queue.get_nowait()
                result = self.process_file(file_path)
                self.results.append(result)
                file_paths_queue.task_done()
            except Empty:
                break

    def process_files_in_parallel(self):
        try:
            futures = [
                self.thread_pool.submit(self._worker, self.file_paths_queue)
                for _ in range(self.thread_pool._max_workers)
            ]
            for future in futures:
                future.result()
        finally:
            self.thread_pool.shutdown(wait=True)

    def check_results_for_failures(self):
        files_failed = []
        for result in self.results:
            if not result[0]:
                files_failed.append(result[1])
        return files_failed

    def results_failure_summary(self):
        files_failed = self.check_results_for_failures()
        if files_failed:
            message = f"Some files failed to process: {', '.join(files_failed)}"
            notifier = NotifierFactory.get_notifier("slack")
            slack_notifier = notifier(
                level=AlertLevel.ERROR,
                title=f"File Processing Failure Summary",
                message=message,
            )
            slack_notifier.notify()

    def __del__(self):
        if hasattr(self, "thread_pool"):
            self.thread_pool.shutdown(wait=True)
        if hasattr(self, "engine"):
            self.engine.dispose()
