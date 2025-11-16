import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator

from sqlalchemy import Table, insert, text
from sqlalchemy.orm import Session

from src.pipeline.db_utils import db_get_column_names
from src.settings import config
from src.sources.base import DataSource

logger = logging.getLogger(__name__)


class BaseWriter(ABC):
    def __init__(
        self,
        source: DataSource,
        session: Session,
        file_load_dlq_table: Table,
    ):
        self.source: DataSource = source
        self.Session: Session = session
        self.columns: list[str] = db_get_column_names(self.source)
        self.batch_size: int = config.BATCH_SIZE
        self.file_load_dlq_table: Table = file_load_dlq_table

    def create_stage_insert_sql(self, stage_table_name: str) -> str:
        placeholders = ", ".join([f":{col}" for col in self.columns])
        return text(
            f"INSERT INTO {stage_table_name} ({', '.join(self.columns)}) VALUES ({placeholders})"
        )

    @abstractmethod
    def write(
        self,
        batches: Iterator[tuple[bool, list[Dict[str, Any]]]],
        stage_table_name: str,
    ):
        sql_insert_template = self.create_stage_insert_sql(stage_table_name)
        valid_records = []
        invalid_records = []
        for batch in batches:
            for passed, record in batch:
                if passed:
                    valid_records.append(record)
                else:
                    invalid_records.append(record)
                if len(valid_records) == self.batch_size:
                    with self.Session() as session:
                        try:
                            session.execute(sql_insert_template, valid_records)
                            session.commit()
                            valid_records = []
                        except Exception as e:
                            logger.exception(
                                f"Error inserting records into stage table {stage_table_name}: {e}"
                            )
                            session.rollback()
                            raise e
                if len(invalid_records) == self.batch_size:
                    with self.Session() as session:
                        try:
                            stmt = insert(self.file_load_dlq_table).values(
                                invalid_records
                            )
                            session.execute(stmt)
                            session.commit()
                            invalid_records = []
                        except Exception as e:
                            logger.exception(
                                f"Error inserting records into file load DLQ table: {e}"
                            )
                            session.rollback()
                            raise e
            if valid_records:
                with self.Session() as session:
                    try:
                        session.execute(sql_insert_template, valid_records)
                        session.commit()
                    except Exception as e:
                        logger.exception(
                            f"Error inserting records into stage table {stage_table_name}: {e}"
                        )
                        session.rollback()
                        raise e
            if invalid_records:
                with self.Session() as session:
                    try:
                        stmt = insert(self.file_load_dlq_table).values(invalid_records)
                        session.execute(stmt)
                        session.commit()
                    except Exception as e:
                        logger.exception(
                            f"Error inserting records into file load DLQ table: {e}"
                        )
                        session.rollback()
                        raise e
