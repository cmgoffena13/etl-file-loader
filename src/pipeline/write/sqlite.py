from sqlalchemy.orm import Session, sessionmaker

from src.pipeline.write.base import BaseWriter
from src.sources.base import DataSource


class SQLiteWriter(BaseWriter):
    def __init__(self, source: DataSource, Session: sessionmaker[Session]):
        super().__init__(source, Session)
