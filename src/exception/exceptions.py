from typing import Any

from src.exception.base import BaseFileErrorEmailException


class DuplicateFileError(BaseFileErrorEmailException):
    def __init__(self, error_values: dict[str, Any]):
        super().__init__(error_values=error_values)

    @property
    def email_message(self) -> str:
        return (
            "The file {source_filename} has already been processed and has been moved to the duplicates directory.\n\n"
            "To reprocess this file:\n"
            "1. Existing records need to be removed from the target table where source_filename = '{source_filename}'\n"
            "2. Move the file from the duplicates directory back to the processing directory"
        )


class GrainValidationError(BaseFileErrorEmailException):
    def __init__(self, error_values: dict[str, Any]):
        super().__init__(error_values=error_values)

    @property
    def email_message(self) -> str:
        return (
            "Grain values are not unique for file: {source_filename}\n"
            "Table: {stage_table_name}\n"
            "Grain columns (file column names): {grain_aliases_formatted}"
        )


class AuditFailedError(BaseFileErrorEmailException):
    def __init__(self, error_values: dict[str, Any]):
        super().__init__(error_values=error_values)

    @property
    def email_message(self) -> str:
        return (
            "Audit checks failed for file: {source_filename}\n"
            "Table: {stage_table_name}\n"
            "Failed audits: \n{failed_audits_formatted}"
        )


class NoDataInFileError(BaseFileErrorEmailException):
    def __init__(self, error_values: dict[str, Any]):
        super().__init__(error_values=error_values)

    @property
    def email_message(self) -> str:
        return "No data found in file: {source_filename}"


class MissingHeaderError(BaseFileErrorEmailException):
    def __init__(self, error_values: dict[str, Any]):
        super().__init__(error_values=error_values)

    @property
    def email_message(self) -> str:
        return "No header found in file: {source_filename}"


class MissingColumnsError(BaseFileErrorEmailException):
    def __init__(self, error_values: dict[str, Any]):
        super().__init__(error_values=error_values)

    @property
    def email_message(self) -> str:
        return (
            "Missing required fields in file: {source_filename}\n"
            "Required fields: {required_fields_display_formatted}\n"
            "Missing fields: {missing_fields_display_formatted}"
        )


class ValidationThresholdExceededError(BaseFileErrorEmailException):
    def __init__(self, error_values: dict[str, Any]):
        super().__init__(error_values=error_values)

    @property
    def email_message(self) -> str:
        return (
            "Validation error rate ({truncated_error_rate}) exceeds threshold ({threshold}) for file: {source_filename} \n"
            "Total Records Processed: {records_validated} \n"
            "Failed Records: {validation_errors} "
        )


class DirectoryNotFoundError(Exception):
    pass


class FileCopyError(Exception):
    pass


class FileMoveError(Exception):
    pass


class MultipleSourcesMatchError(Exception):
    pass


class FileDeleteError(Exception):
    pass
