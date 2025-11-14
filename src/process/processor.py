import multiprocessing
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from process.file_helper import FileHelper
from src.pipeline.runner import PipelineRunner
from src.settings import config
from src.sources.master import MASTER_REGISTRY


class Processor:
    def __init__(self):
        self.thread_pool = ThreadPoolExecutor(max_workers=multiprocessing.cpu_count())
        self.files = FileHelper.scan_directory(config.DIRECTORY_PATH)
        self.thread_batches = []

    def process_file(self, file_path: Path):
        FileHelper.copy_file_to_archive(file_path)
        try:
            source = MASTER_REGISTRY.find_source_for_file(file_path)
            if source is not None:
                runner = PipelineRunner(file_path, source)
                runner.run()
        finally:
            file_path.unlink()

    def process_files_in_parallel(self):
        num_threads = self.thread_pool._max_workers
        files_per_thread = len(self.files) // num_threads
        remainder = len(self.files) % num_threads

        start = 0
        for index in range(num_threads):
            batch_size = files_per_thread + (1 if index < remainder else 0)
            end = start + batch_size
            batch = self.files[start:end]
            if batch:  # Only add non-empty batches
                self.thread_batches.append(batch)
            start = end
        try:
            batch_futures = [
                self.thread_pool.submit(self.process_file, batch)
                for batch in self.thread_batches
            ]
            batch_results = [future.result() for future in batch_futures]
        finally:
            self.thread_pool.shutdown(wait=True)

    def __del__(self):
        if hasattr(self, "thread_pool"):
            self.thread_pool.shutdown(wait=True)
