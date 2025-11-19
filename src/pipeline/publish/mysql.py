import pendulum
from sqlalchemy import Engine, text

from src.pipeline.publish.base import BasePublisher
from src.sources.base import DataSource


class MySQLPublisher(BasePublisher):
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
        # Convert ISO datetime to MySQL datetime format
        now_iso = pendulum.parse(now_iso).format("YYYY-MM-DD HH:mm:ss.SSSSSS")

        insert_columns = ", ".join(self.columns) + ", etl_created_at"
        select_columns = (
            ", ".join([f"stage.{col}" for col in self.columns]) + f", '{now_iso}'"
        )
        update_columns_filtered = [
            col for col in self.update_columns if col != "source_filename"
        ]
        update_on_duplicate_parts = []
        for col in update_columns_filtered:
            update_on_duplicate_parts.append(f"{col} = stage.{col}")

        # Only update source_filename and etl_updated_at if data actually changed
        update_on_duplicate_parts.append(
            f"source_filename = IF(stage.etl_row_hash != {self.target_table_name}.etl_row_hash, stage.source_filename, {self.target_table_name}.source_filename)"
        )
        update_on_duplicate_parts.append(
            f"etl_updated_at = IF(stage.etl_row_hash != {self.target_table_name}.etl_row_hash, '{now_iso}', {self.target_table_name}.etl_updated_at)"
        )
        update_on_duplicate = ", ".join(update_on_duplicate_parts)

        return text(f"""
            INSERT INTO {self.target_table_name} ({insert_columns})
            SELECT {select_columns}
            FROM {self.stage_table_name} AS stage
            ON DUPLICATE KEY UPDATE
                {update_on_duplicate}
        """)
