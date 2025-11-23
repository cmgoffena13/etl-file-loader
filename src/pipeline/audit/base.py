import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Union

from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker

from src.exception.exceptions import AuditFailedError, GrainValidationError
from src.pipeline.db_utils import db_create_duplicate_grain_examples_sql
from src.pipeline.model_utils import get_field_alias
from src.sources.base import DataSource
from src.utils import get_file_name, retry

logger = logging.getLogger(__name__)


class BaseAuditor(ABC):
    def __init__(
        self,
        file_path: Union[Path, str],
        source: DataSource,
        engine: Engine,
        stage_table_name: str,
        log_id: int,
    ):
        self.file_path: Union[Path, str] = file_path
        self.source_filename: str = get_file_name(file_path)
        self.stage_table_name: str = stage_table_name
        self.source: DataSource = source
        self.engine: Engine = engine
        self.audit_query: str = source.audit_query
        self.failed_audits: list[str] = []
        self.Session: sessionmaker[Session] = sessionmaker(bind=engine)
        self.log_id: int = log_id

    def _get_duplicate_grain_examples(self, session: Session):
        duplicate_sql = db_create_duplicate_grain_examples_sql(self.source)
        duplicate_sql = duplicate_sql.format(table=self.stage_table_name)
        return session.execute(text(duplicate_sql)).fetchall()

    def _format_duplicate_examples(
        self, duplicate_examples: list[dict[str, Any]]
    ) -> str:
        grain_field_aliases = {
            grain_field: get_field_alias(self.source, grain_field)
            for grain_field in self.source.grain
        }
        duplicate_examples_formatted = "Sample duplicate grain violations:\n"
        for record in duplicate_examples:
            record_dict = dict(record._mapping)
            aliased_record = {
                grain_field_aliases[grain_field]: record_dict[grain_field]
                for grain_field in self.source.grain
            }
            aliased_record["duplicate_count"] = record_dict["duplicate_count"]
            record_str = ", ".join(f"{k}: {v}" for k, v in aliased_record.items())
            duplicate_examples_formatted += f"  - {record_str}\n"
        return duplicate_examples_formatted

    def _raise_grain_validation_error(self, duplicate_examples: list[dict[str, Any]]):
        grain_field_aliases = {
            grain_field: get_field_alias(self.source, grain_field)
            for grain_field in self.source.grain
        }
        duplicate_examples_formatted = self._format_duplicate_examples(
            duplicate_examples
        )
        logger.error(
            f"[log_id={self.log_id}] Grain validation failed for table {self.stage_table_name}"
        )
        raise GrainValidationError(
            error_values={
                "stage_table_name": self.stage_table_name,
                "grain_aliases_formatted": ", ".join(grain_field_aliases.values()),
                "additional_details": duplicate_examples_formatted,
            }
        )

    @abstractmethod
    def create_grain_validation_sql(self) -> str:
        pass

    @retry()
    def audit_grain(self):
        logger.info(
            f"[log_id={self.log_id}] Auditing grain for table {self.stage_table_name}"
        )
        grain_sql = self.create_grain_validation_sql()
        grain_sql = grain_sql.format(table=self.stage_table_name)
        with self.Session() as session:
            result = session.execute(text(grain_sql)).fetchone()
            if result._mapping["grain_unique"] == 0:
                duplicate_examples = self._get_duplicate_grain_examples(session)
                self._raise_grain_validation_error(duplicate_examples)

    @retry()
    def audit_data(self):
        if self.audit_query is None:
            logger.warning(
                f"[log_id={self.log_id}] No audit query found for source {self.source.table_name}"
            )
            return

        with self.Session() as session:
            logger.info(
                f"[log_id={self.log_id}] Auditing data for table {self.stage_table_name}"
            )
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
            logger.error(
                f"[log_id={self.log_id}] Audit failed for table {self.stage_table_name}"
            )
            raise AuditFailedError(
                error_values={
                    "stage_table_name": self.stage_table_name,
                    "failed_audits_formatted": failed_audits_formatted,
                }
            )
