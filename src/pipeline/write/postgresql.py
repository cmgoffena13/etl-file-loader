import logging

from sqlalchemy.orm import Session

from src.pipeline.write.base import BaseWriter
from src.sources.base import DataSource

logger = logging.getLogger(__name__)


class PostgreSQLWriter(BaseWriter):
    def __init__(self, source: DataSource, session: Session):
        super().__init__(source, session)
