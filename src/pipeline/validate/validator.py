import logging
from pathlib import Path
from typing import Any, Dict, Iterator, List

import pendulum
from pydantic import TypeAdapter, ValidationError

from src.exception.exceptions import ValidationThresholdExceededError
from src.pipeline.db_utils import db_create_row_hash, db_serialize_json_for_dlq_table
from src.pipeline.model_utils import (
    create_field_mapping,
    create_reverse_field_mapping,
    create_sorted_keys,
    extract_failed_field_names,
    extract_validation_error_message,
    rename_keys_and_filter_record,
)
from src.settings import config
from src.sources.base import DataSource

logger = logging.getLogger(__name__)


class Validator:
    def __init__(
        self, file_path: Path, source: DataSource, starting_row_number: int, log_id: int
    ):
        self.source: DataSource = source
        self.field_mapping: Dict[str, str] = create_field_mapping(source)
        self.reverse_field_mapping: Dict[str, str] = create_reverse_field_mapping(
            source
        )
        self.adapter: TypeAdapter[Any] = TypeAdapter(source.source_model)
        self.sorted_keys: tuple[str] = create_sorted_keys(source)
        self.records_validated: int = 0
        self.validation_errors: int = 0
        self.starting_row_number: int = starting_row_number
        self.source_filename: str = file_path.name
        self.batch_size: int = config.BATCH_SIZE
        self.sample_validation_errors: List[Dict[str, Any]] = []
        self.log_id: int = log_id

    def _create_dlq_record(
        self,
        record: Dict[str, Any],
        failed_field_names: set[str],
        error_details: List[Dict[str, Any]],
        index: int,
    ) -> Dict[str, Any]:
        file_record_data: Dict[str, Any] = {
            self.reverse_field_mapping.get(field_name, field_name): value
            for field_name, value in record.items()
            if field_name in failed_field_names
        }
        record = {
            "file_record_data": db_serialize_json_for_dlq_table(file_record_data),
            "validation_errors": db_serialize_json_for_dlq_table(
                extract_validation_error_message(
                    error_details, self.reverse_field_mapping
                )
            ),
            "file_row_number": index,
            "source_filename": self.source_filename,
            "file_load_log_id": self.log_id,
            "target_table_name": self.source.table_name,
            "failed_at": pendulum.now("UTC"),
        }
        return record

    def validate(
        self, batches: Iterator[list[Dict[str, Any]]]
    ) -> Iterator[list[tuple[bool, Dict[str, Any]]]]:
        total_records = 0
        for batch in batches:
            batch_results = [None] * self.batch_size
            batch_index = 0
            for record in batch:
                total_records += 1
                record = rename_keys_and_filter_record(record, self.field_mapping)
                try:
                    record = self.adapter.validate_python(record).model_dump()
                    record["etl_row_hash"] = db_create_row_hash(
                        record, sorted_keys=self.sorted_keys
                    )
                    record["source_filename"] = self.source_filename
                    record["file_load_log_id"] = self.log_id
                    batch_results[batch_index] = (True, record)
                except ValidationError as e:
                    self.validation_errors += 1
                    error_details = (
                        e.errors() if hasattr(e, "errors") else [{"msg": str(e)}]
                    )
                    failed_field_names = extract_failed_field_names(
                        e, self.source.grain
                    )
                    file_row_number = total_records - self.starting_row_number + 1
                    dlq_record = self._create_dlq_record(
                        record, failed_field_names, error_details, file_row_number
                    )
                    batch_results[batch_index] = (False, dlq_record)

                    # Collect sample errors (first 5)
                    if len(self.sample_validation_errors) < 5:
                        self.sample_validation_errors.append(
                            {
                                "file_row_number": file_row_number,
                                "validation_error": extract_validation_error_message(
                                    error_details, self.reverse_field_mapping
                                ),
                                "record": record,
                            }
                        )
                batch_index += 1
                self.records_validated += 1
                if batch_index == self.batch_size:
                    logger.info(
                        f"[log_id={self.log_id}] Validated batch of {self.batch_size} rows"
                    )
                    yield batch_results
                    batch_results[:] = [None] * self.batch_size
                    batch_index = 0
            if batch_index > 0:
                logger.info(
                    f"[log_id={self.log_id}] Validated final batch of {batch_index} rows"
                )
                yield batch_results[:batch_index]

    def check_validation_threshold(self) -> None:
        if self.records_validated > 0 and self.validation_errors > 0:
            error_rate = self.validation_errors / self.records_validated
            if error_rate > self.source.validation_error_threshold:
                truncated_error_rate = round(error_rate, 2)
                sample_errors_str = "Sample validation failure records:\n" + "\n".join(
                    f"Row {err['file_row_number']}: {err['validation_error']} - Record: {err['record']}"
                    for err in self.sample_validation_errors
                )
                logger.error(
                    f"[log_id={self.log_id}] Validation threshold exceeded: {truncated_error_rate} > {self.source.validation_error_threshold}"
                )
                raise ValidationThresholdExceededError(
                    error_values={
                        "truncated_error_rate": truncated_error_rate,
                        "threshold": self.source.validation_error_threshold,
                        "records_validated": self.records_validated,
                        "validation_errors": self.validation_errors,
                        "additional_details": sample_errors_str,
                    }
                )
