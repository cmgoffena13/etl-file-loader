from sqlalchemy import Engine

from src.pipeline.publish.base import BasePublisher
from src.pipeline.publish.bigquery import BigQueryPublisher
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
        "bigquery": BigQueryPublisher,
    }

    @classmethod
    def get_supported_extensions(cls) -> list[str]:
        return list[str](cls._publishers.keys())

    @classmethod
    def create_publisher(
        cls,
        source: DataSource,
        engine: Engine,
        log_id: int,
        stage_table_name: str,
        rows_written_to_stage: int,
    ) -> BasePublisher:
        try:
            publisher_class = cls._publishers[config.DRIVERNAME]
        except KeyError:
            raise ValueError(
                f"Unsupported database driver for publisher: {config.DRIVERNAME}. Supported drivers: {cls.get_supported_extensions()}"
            )
        return publisher_class(
            source, engine, log_id, stage_table_name, rows_written_to_stage
        )
