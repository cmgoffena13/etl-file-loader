import logging
from abc import ABC, abstractmethod

from sqlalchemy import Engine, Table
from sqlalchemy.orm import Session, sessionmaker

from src.sources.base import DataSource

logger = logging.getLogger(__name__)


class BasePublisher(ABC):
    def __init__(
        self,
        source: DataSource,
        engine: Engine,
        file_load_dlq_table: Table,
        log_id: int,
    ):
        self.source: DataSource = source
        self.engine: Engine = engine
        self.Session: sessionmaker[Session] = sessionmaker(bind=engine)
        self.log_id: int = log_id

    @abstractmethod
    def create_publish_sql(self):
        pass

    @abstractmethod
    def publish_data(self):
        pass
