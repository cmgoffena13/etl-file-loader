from sqlalchemy.orm import Session, sessionmaker

from src.pipeline.write.base import BaseWriter
from src.sources.base import DataSource


class MySQLWriter(BaseWriter):
    def __init__(self, source: DataSource, Session: sessionmaker[Session]):
        super().__init__(source, Session)
