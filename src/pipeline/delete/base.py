import logging
from abc import ABC, abstractmethod

from sqlalchemy import Engine, Table, select
from sqlalchemy.orm import Session, sessionmaker

from src.settings import config
from src.sources.base import DataSource
from src.utils import retry

logger = logging.getLogger(__name__)


class BaseDeleter(ABC):
    def __init__(
        self,
        source_filename: str,
        engine: Engine,
        log_id: int,
        file_load_dlq_table: Table,
    ):
        self.source_filename: str = source_filename
        self.engine: Engine = engine
        self.log_id: int = log_id
        self.Session: sessionmaker[Session] = sessionmaker(bind=engine)
        self.file_load_dlq_table: Table = file_load_dlq_table

    @abstractmethod
    def create_delete_sql(self) -> str:
        pass

    @retry
    def _check_if_dlq_records_exist(self) -> bool:
        with self.Session() as session:
            return session.execute(
                select(self.file_load_dlq_table.c.id)
                .where(
                    self.file_load_dlq_table.c.source_filename == self.source_filename,
                    self.file_load_dlq_table.c.file_load_log_id < self.log_id,
                )
                .limit(1)
            ).first()

    @retry
    def _batch_delete_dlq_records(self):
        with self.Session() as session:
            delete_sql = self.create_delete_sql()
            total_deleted = 0

            try:
                while True:
                    result = session.execute(
                        delete_sql,
                        {"file_name": self.source_filename, "limit": config.BATCH_SIZE},
                    )
                    session.commit()

                    if result.rowcount == 0:
                        break

                    total_deleted += result.rowcount

                logger.info(
                    f"[log_id={self.log_id}] Deleted total of {total_deleted} DLQ record(s) for file: {self.source_filename}"
                )
            except Exception as e:
                session.rollback()
                logger.exception(
                    f"[log_id={self.log_id}] Failed to delete DLQ records for file: {self.source_filename}: {e}"
                )
                raise

    def delete(self):
        existing_dlq = self._check_if_dlq_records_exist()
        if existing_dlq:
            self._batch_delete_dlq_records()
