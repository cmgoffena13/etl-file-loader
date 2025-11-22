from pathlib import Path
from typing import Union

from sqlalchemy import Engine

from src.pipeline.audit.base import BaseAuditor
from src.pipeline.audit.mssql import SQLServerAuditor
from src.pipeline.audit.mysql import MySQLAuditor
from src.pipeline.audit.postgresql import PostgreSQLAuditor
from src.pipeline.audit.sqlite import SQLiteAuditor
from src.settings import config
from src.sources.base import DataSource


class AuditorFactory:
    _auditors = {
        "mssql": SQLServerAuditor,
        "postgresql": PostgreSQLAuditor,
        "mysql": MySQLAuditor,
        "sqlite": SQLiteAuditor,
    }

    @classmethod
    def get_supported_extensions(cls) -> list[str]:
        return list[str](cls._auditors.keys())

    @classmethod
    def create_auditor(
        cls,
        file_path: Union[Path, str],
        source: DataSource,
        engine: Engine,
        stage_table_name: str,
        log_id: int,
    ) -> BaseAuditor:
        try:
            auditor_class = cls._auditors[config.DRIVERNAME]
        except KeyError:
            raise ValueError(
                f"Unsupported database driver for auditor: {config.DRIVERNAME}. Supported drivers: {cls.get_supported_extensions()}"
            )
        return auditor_class(file_path, source, engine, stage_table_name, log_id)
