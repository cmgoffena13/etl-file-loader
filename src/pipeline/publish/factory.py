from src.pipeline.publish.base import BasePublisher
from src.pipeline.publish.mssql import SQLServerPublisher
from src.pipeline.publish.mysql import MySQLPublisher
from src.pipeline.publish.postgresql import PostgreSQLPublisher
from src.pipeline.publish.sqlite import SQLitePublisher
from src.settings import config
from src.sources.base import DataSource


class PublisherFactory:
    _publishers = {
        "mssql": SQLServerPublisher,
        "postgresql": PostgreSQLPublisher,
        "mysql": MySQLPublisher,
        "sqlite": SQLitePublisher,
    }

    @classmethod
    def create_publisher(
        cls, source: DataSource, engine: Engine, log_id: int
    ) -> BasePublisher:
        return cls._publishers[config.DRIVERNAME](source, engine, log_id)
