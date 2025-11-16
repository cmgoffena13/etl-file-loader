import logging
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field

from src.exception.exceptions import MultipleSourcesMatchError
from src.pipeline.read.factory import ReaderFactory
from src.sources.base import DataSource
from src.utils import get_file_extension

logger = logging.getLogger(__name__)


class SourceRegistry(BaseModel):
    sources: list[DataSource] = Field(default_factory=list)

    def add_sources(self, sources: list[DataSource]) -> None:
        self.sources.extend(sources)

    def find_source_for_file(self, file_path: Path) -> Optional[DataSource]:
        # 1: Get reader class for file extension
        extension = get_file_extension(file_path)
        if extension not in ReaderFactory._readers:
            logger.warning(
                f"Unsupported file extension: {extension}. "
                f"Supported extensions: {ReaderFactory.get_supported_extensions()}"
            )
            return None

        expected_source_type = ReaderFactory._readers[extension].SOURCE_TYPE

        # 2: Filter sources by type and file pattern
        matching_sources = [
            source
            for source in self.sources
            if isinstance(source, expected_source_type)
            and source.matches_file(str(file_path))
        ]

        if len(matching_sources) == 0:
            logger.warning(f"No source configuration found for file '{file_path}'. ")
            return None
        elif len(matching_sources) == 1:
            return matching_sources[0]
        else:
            source_names = [s.table_name for s in matching_sources]
            raise MultipleSourcesMatchError(
                f"Multiple sources match file '{file_path.name}': {', '.join(source_names)}"
            )
