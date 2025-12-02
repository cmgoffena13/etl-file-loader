import pendulum
import structlog
from sqlalchemy import Engine, text

from src.pipeline.publish.base import BasePublisher
from src.sources.base import DataSource

logger = structlog.getLogger(__name__)


class PostgreSQLPublisher(BasePublisher):
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

    def create_publish_sql(self, now_iso: str):
        insert_columns = ", ".join(self.columns) + ", etl_created_at"
        insert_values = (
            ", ".join(f"stage.{col}" for col in self.columns) + f", '{now_iso}'"
        )
        update_set = (
            ", ".join(f"{col} = stage.{col}" for col in self.update_columns)
            + f", etl_updated_at = '{now_iso}'"
        )

        return text(f"""
            MERGE INTO {self.target_table_name} AS target
            USING {self.stage_table_name} AS stage
            ON {self.join_condition}
            WHEN MATCHED AND stage.etl_row_hash != target.etl_row_hash THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns})
                VALUES ({insert_values});
        """)
