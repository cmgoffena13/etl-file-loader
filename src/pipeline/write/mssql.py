from sqlalchemy.orm import Session

from src.pipeline.write.base import BaseWriter
from src.sources.base import DataSource


class SQLServerWriter(BaseWriter):
    def __init__(self, source: DataSource, session: Session):
        super().__init__(source, session)
