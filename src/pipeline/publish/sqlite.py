from sqlalchemy import Engine

from src.pipeline.publish.base import BasePublisher
from src.sources.base import DataSource


class SQLitePublisher(BasePublisher):
    def __init__(
        self,
        source: DataSource,
        engine: Engine,
        log_id: int,
        stage_table_name: str,
        rows_written_to_stage: int,
    ):
        super().__init__(
            source, engine, log_id, stage_table_name, rows_written_to_stage
        )

    def create_publish_sql(self):
        pass

    def publish_data(self):
        pass
