from sqlalchemy import Engine, text

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

    def create_publish_sql(self, now_iso: str):
        insert_columns = ", ".join(self.columns) + ", etl_created_at"
        select_columns = (
            ", ".join([f"stage.{col}" for col in self.columns]) + f", '{now_iso}'"
        )
        conflict_columns = ", ".join(self.source.grain)
        update_columns_filtered = [
            col for col in self.update_columns if col != "source_filename"
        ]
        update_set_parts = []
        for col in update_columns_filtered:
            update_set_parts.append(f"{col} = excluded.{col}")
        # Only update source_filename and etl_updated_at if data actually changed
        update_set_parts.append(
            f"source_filename = CASE WHEN excluded.etl_row_hash != etl_row_hash THEN excluded.source_filename ELSE source_filename END"
        )
        update_set_parts.append(
            f"etl_updated_at = CASE WHEN excluded.etl_row_hash != etl_row_hash THEN excluded.etl_updated_at ELSE etl_updated_at END"
        )
        update_set = ", ".join(update_set_parts)

        return text(f"""
            INSERT INTO {self.target_table_name} ({insert_columns})
            SELECT {select_columns}
            FROM {self.stage_table_name} AS stage
            WHERE 1=1
            ON CONFLICT({conflict_columns})
            DO UPDATE SET
                {update_set}
        """)
