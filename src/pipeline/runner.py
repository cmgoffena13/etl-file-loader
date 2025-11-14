import logging
from pathlib import Path
from typing import Any, Dict, Iterator

from src.pipeline.read.factory import ReaderFactory
from src.sources.base import DataSource

logger = logging.getLogger(__name__)


class PipelineRunner:
    def __init__(self, file_path: Path, data_source: DataSource):
        self.data_source = data_source
        self.reader = ReaderFactory.create_reader(file_path, data_source)

    def read_data(self) -> Iterator[list[Dict[str, Any]]]:
        pass

    def validate_data(self) -> Iterator[list[Dict[str, Any]]]:
        pass

    def write_data(self) -> Iterator[list[Dict[str, Any]]]:
        pass

    def audit_data(self):
        pass

    def publish_data(self):
        pass

    def run(self):
        self.write_data(self.validate_data(self.read_data()))
        self.audit_data()
        self.publish_data()
