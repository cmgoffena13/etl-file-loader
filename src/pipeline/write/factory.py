from typing import Type

from sqlalchemy import Engine, MetaData, Table
from sqlalchemy.orm import Session, sessionmaker

from src.pipeline.write.base import BaseWriter
from src.pipeline.write.mssql import SQLServerWriter
from src.pipeline.write.mysql import MySQLWriter
from src.pipeline.write.postgresql import PostgreSQLWriter
from src.pipeline.write.sqlite import SQLiteWriter
from src.settings import config
from src.sources.base import DataSource


class WriterFactory:
    _writers = {
        "mssql": SQLServerWriter,
        "postgresql": PostgreSQLWriter,
        "mysql": MySQLWriter,
        "sqlite": SQLiteWriter,
    }

    @classmethod
    def get_supported_extensions(cls) -> list[str]:
        return list[str](cls._writers.keys())

    @classmethod
    def create_writer(
        cls, source: DataSource, engine: Engine, file_load_dlq_table: Table
    ) -> BaseWriter:
        try:
            writer_class = cls._writers[config.DRIVERNAME]
        except KeyError:
            raise ValueError(
                f"Unsupported database driver for writer: {config.DRIVERNAME}. Supported drivers: {cls.get_supported_extensions()}"
            )
        return writer_class(source, engine, file_load_dlq_table)
