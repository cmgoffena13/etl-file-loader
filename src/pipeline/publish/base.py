from abc import ABC, abstractmethod

import pendulum
import structlog
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker

from src.pipeline.db_utils import db_get_column_names
from src.sources.base import DataSource

logger = structlog.getLogger(__name__)


class BasePublisher(ABC):
    def __init__(
        self,
        source: DataSource,
        engine: Engine,
        log_id: int,
        stage_table_name: str,
        rows_written_to_stage: int,
    ):
        self.source: DataSource = source
        self.engine: Engine = engine
        self.Session: sessionmaker[Session] = sessionmaker(bind=engine)
        self.log_id: int = log_id
        self.columns: list[str] = db_get_column_names(self.source)
        self.join_condition: str = " AND ".join(
            [f"target.{col} = stage.{col}" for col in self.source.grain]
        )
        self.update_columns: list[str] = [
            col for col in self.columns if col not in self.source.grain
        ]
        self.stage_table_name: str = stage_table_name
        self.target_table_name: str = self.source.table_name
        self.rows_written_to_stage: int = rows_written_to_stage
        self.publish_inserts: int = 0
        self.publish_updates: int = 0

    def _insert_count_sql(self) -> str:
        return text(f"""
                SELECT 
                COUNT(*) 
                FROM {self.stage_table_name} AS stage
                WHERE EXISTS (
                    SELECT 1 
                    FROM {self.target_table_name} AS target
                    WHERE {self.join_condition}
                )""")

    def get_insert_count(self) -> int:
        with self.Session() as session:
            records_that_exist_in_target = session.execute(
                self._insert_count_sql()
            ).scalar()
            # EXISTS is more efficient than NOT EXISTS
            return self.rows_written_to_stage - records_that_exist_in_target

    def _update_count_sql(self) -> str:
        return text(f"""
                SELECT 
                COUNT(*) 
                FROM {self.stage_table_name} AS stage
                WHERE EXISTS (
                    SELECT 1 
                    FROM {self.target_table_name} AS target
                    WHERE {self.join_condition}
                    AND stage.etl_row_hash != target.etl_row_hash
                ) 
                """)

    def get_update_count(self) -> int:
        with self.Session() as session:
            return session.execute(self._update_count_sql()).scalar()

    @abstractmethod
    def create_publish_sql(self, now_iso: str):
        pass

    def publish(self):
        self.publish_inserts = self.get_insert_count()
        self.publish_updates = self.get_update_count()

        now_iso = pendulum.now("UTC").isoformat()
        with self.Session() as session:
            try:
                logger.info(
                    f"Publishing {self.stage_table_name} to {self.target_table_name}"
                )
                session.execute(self.create_publish_sql(now_iso))
                session.commit()
            except Exception as e:
                logger.exception(
                    f"Failed to publish {self.stage_table_name} to {self.target_table_name}: {e}"
                )
                session.rollback()
                raise e
