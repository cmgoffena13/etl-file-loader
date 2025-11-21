import logging
from abc import ABC
from typing import Any, Dict, Iterator

from sqlalchemy import Engine, Table, insert, text
from sqlalchemy.orm import Session, sessionmaker

from src.pipeline.db_utils import db_get_column_names
from src.settings import config
from src.sources.base import DataSource

logger = logging.getLogger(__name__)


class BaseWriter(ABC):
    def __init__(
        self,
        source: DataSource,
        engine: Engine,
        file_load_dlq_table: Table,
        log_id: int,
        stage_table_name: str,
    ):
        self.source: DataSource = source
        self.engine: Engine = engine
        self.Session: sessionmaker[Session] = sessionmaker(bind=engine)
        self.columns: list[str] = db_get_column_names(self.source)
        self.batch_size: int = config.BATCH_SIZE
        self.file_load_dlq_table: Table = file_load_dlq_table
        self.rows_written_to_stage: int = 0
        self.log_id: int = log_id
        self.stage_table_name: str = stage_table_name

    def create_stage_insert_sql(self) -> str:
        placeholders = ", ".join([f":{col}" for col in self.columns])
        return text(
            f"INSERT INTO {self.stage_table_name} ({', '.join(self.columns)}) VALUES ({placeholders})"
        )

    def write(self, batches: Iterator[tuple[bool, list[Dict[str, Any]]]]) -> None:
        sql_insert_template = self.create_stage_insert_sql()
        valid_records = [None] * self.batch_size
        valid_index = 0
        invalid_records = []
        for batch in batches:
            for passed, record in batch:
                if passed:
                    valid_records[valid_index] = record
                    valid_index += 1
                    if valid_index == self.batch_size:
                        with self.Session() as session:
                            try:
                                logger.debug(
                                    f"[log_id={self.log_id}] Writing batch of {len(valid_records)} rows to stage table {self.stage_table_name}"
                                )
                                session.execute(sql_insert_template, valid_records)
                                session.commit()
                                self.rows_written_to_stage += len(valid_records)

                                valid_records[:] = [None] * self.batch_size
                                valid_index = 0
                            except Exception as e:
                                logger.exception(
                                    f"Error inserting records into stage table {self.stage_table_name}: {e}"
                                )
                                session.rollback()
                                raise e
                else:
                    invalid_records.append(record)
                    if len(invalid_records) == self.batch_size:
                        with self.Session() as session:
                            try:
                                stmt = insert(self.file_load_dlq_table).values(
                                    invalid_records
                                )
                                session.execute(stmt)
                                session.commit()
                                self.rows_written_to_stage += len(invalid_records)
                                invalid_records.clear()
                            except Exception as e:
                                logger.exception(
                                    f"Error inserting records into file load DLQ table: {e}"
                                )
                                session.rollback()
                                raise e
            if (
                self.rows_written_to_stage % 100000 == 0
                or self.rows_written_to_stage < 100000
            ) and self.rows_written_to_stage > 0:
                logger.info(
                    f"[log_id={self.log_id}] Rows written: {self.rows_written_to_stage}"
                )
            if valid_index > 0:
                with self.Session() as session:
                    try:
                        logger.debug(
                            f"[log_id={self.log_id}] Writing final batch of {valid_index} rows to stage table {self.stage_table_name}"
                        )
                        session.execute(
                            sql_insert_template, valid_records[:valid_index]
                        )
                        session.commit()
                        self.rows_written_to_stage += valid_index
                    except Exception as e:
                        logger.exception(
                            f"Error inserting records into stage table {self.stage_table_name}: {e}"
                        )
                        session.rollback()
                        raise e
            if invalid_records:
                with self.Session() as session:
                    try:
                        stmt = insert(self.file_load_dlq_table).values(invalid_records)
                        session.execute(stmt)
                        session.commit()
                        self.rows_written_to_stage += len(invalid_records)
                    except Exception as e:
                        logger.exception(
                            f"Error inserting records into file load DLQ table: {e}"
                        )
                        session.rollback()
                        raise e
