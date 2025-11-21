from sqlalchemy import Engine, Table, text

from src.pipeline.delete.base import BaseDeleter


class SQLiteDeleter(BaseDeleter):
    def __init__(
        self,
        source_filename: str,
        engine: Engine,
        log_id: int,
        file_load_dlq_table: Table,
    ):
        super().__init__(source_filename, engine, log_id, file_load_dlq_table)

    def create_delete_sql(self) -> str:
        return text(f"""
            DELETE FROM file_load_dlq 
            WHERE id IN (
                SELECT id FROM file_load_dlq 
                WHERE source_filename = :file_name 
                LIMIT :limit
            )
        """)
