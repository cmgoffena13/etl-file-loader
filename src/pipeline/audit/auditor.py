import logging
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from src.exception.exceptions import AuditFailedError
from src.pipeline.db_utils import (
    db_create_duplicate_grain_examples_sql,
    db_create_grain_validation_sql,
)
from src.sources.base import DataSource

logger = logging.getLogger(__name__)


class Auditor:
    def __init__(
        self,
        file_path: Path,
        source: DataSource,
        Session: sessionmaker[Session],
        stage_table_name: str,
    ):
        self.source: DataSource = source
        self.stage_table_name: str = stage_table_name
        self.source_filename: str = file_path.name
        self.Session: sessionmaker[Session] = Session
        self.audit_query: str = source.audit_query
        self.failed_audits: list[str] = []

    def _get_duplicate_grain_examples(self):
        duplicate_sql = db_create_duplicate_grain_examples_sql(self.source)
        duplicate_sql = duplicate_sql.format(table=self.stage_table_name)
        with self.Session() as session:
            return session.execute(text(duplicate_sql)).fetchall()

    def audit_grain(self):
        grain_sql = db_create_grain_validation_sql(self.source)
        grain_sql = grain_sql.format(table=self.stage_table_name)
        with self.Session() as session:
            result = session.execute(text(grain_sql)).fetchone()
            if result._mapping["grain_unique"] == 0:
                duplicate_examples = self._get_duplicate_grain_examples()

    def audit_data(self):
        if self.audit_query is None:
            logger.warning(f"No audit query found for source {self.source.table_name}")
            return

        with self.Session() as session:
            audit_sql = text(
                self.audit_query.format(table=self.stage_table_name).strip()
            )
            result = session.execute(audit_sql).fetchone()
            column_names = list(result._mapping.keys())
        for audit_name in column_names:
            value = result._mapping[audit_name]
            if value == 0:
                self.failed_audits.append(audit_name)
        if self.failed_audits:
            failed_audits_formatted = ", ".join(self.failed_audits)
            raise AuditFailedError(
                error_values={
                    "source_filename": self.source_filename,
                    "stage_table_name": self.stage_table_name,
                    "failed_audits_formatted": failed_audits_formatted,
                }
            )
