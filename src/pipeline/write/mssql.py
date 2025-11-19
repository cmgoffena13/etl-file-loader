from sqlalchemy import Engine, Table

from src.pipeline.write.base import BaseWriter
from src.sources.base import DataSource


class SQLServerWriter(BaseWriter):
    def __init__(
        self,
        source: DataSource,
        engine: Engine,
        file_load_dlq_table: Table,
        log_id: int,
    ):
        super().__init__(source, engine, file_load_dlq_table, log_id)
