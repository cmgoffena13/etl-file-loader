from sqlalchemy import Engine, Table

from src.pipeline.delete.base import BaseDeleter
from src.pipeline.delete.mssql import SQLServerDeleter
from src.pipeline.delete.mysql import MySQLDeleter
from src.pipeline.delete.postgresql import PostgreSQLDeleter
from src.pipeline.delete.sqlite import SQLiteDeleter
from src.settings import config
from src.sources.base import DataSource


class DeleterFactory:
    _deleters = {
        "mssql": SQLServerDeleter,
        "postgresql": PostgreSQLDeleter,
        "mysql": MySQLDeleter,
        "sqlite": SQLiteDeleter,
    }

    @classmethod
    def get_supported_extensions(cls) -> list[str]:
        return list[str](cls._deleters.keys())

    @classmethod
    def create_deleter(
        cls,
        source_filename: str,
        engine: Engine,
        log_id: int,
        file_load_dlq_table: Table,
    ) -> BaseDeleter:
        try:
            deleter_class = cls._deleters[config.DRIVERNAME]
        except KeyError:
            raise ValueError(
                f"Unsupported database driver for deleter: {config.DRIVERNAME}. Supported drivers: {cls.get_supported_extensions()}"
            )
        return deleter_class(source_filename, engine, log_id, file_load_dlq_table)
