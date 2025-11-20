import logging

from sqlalchemy import Engine, Table

from src.pipeline.write.base import BaseWriter
from src.sources.base import DataSource

logger = logging.getLogger(__name__)


class PostgreSQLWriter(BaseWriter):
    def __init__(
        self,
        source: DataSource,
        engine: Engine,
        file_load_dlq_table: Table,
        log_id: int,
        stage_table_name: str,
    ):
        super().__init__(source, engine, file_load_dlq_table, log_id, stage_table_name)
