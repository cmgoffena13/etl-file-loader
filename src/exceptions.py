class DuplicateFileError(Exception):
    error_type = "Duplicate File Detected"
    email_message = (
        "The file {source_filename} has already been processed and has been moved to the duplicates directory.\n\n"
        "To reprocess this file:\n"
        "1. Existing records need to be removed from the target table where source_filename = '{source_filename}'\n"
        "2. Move the file from the duplicates directory back to the processing directory"
    )


class GrainValidationError(Exception):
    error_type = "Grain Validation Error"
    email_message = ""


class AuditFailedError(Exception):
    error_type = "Audit Failed"
    email_message = ""


class MissingHeaderError(Exception):
    error_type = "Missing Header"
    email_message = ""


class MissingColumnsError(Exception):
    error_type = "Missing Columns"
    email_message = ""


class ValidationThresholdExceededError(Exception):
    error_type = "Validation Threshold Exceeded"
    email_message = (
        "Validation error rate ({truncated_error_rate}) exceeds threshold "
        "({threshold}) for file: {source_filename}. "
        "Total Records Processed: {records_validated}, "
        "Failed Records: {validation_errors}. "
    )


# File-specific errors that should not be retried and are handled via email notifications
FILE_ERROR_EXCEPTIONS = {
    MissingHeaderError,
    MissingColumnsError,
    ValidationThresholdExceededError,
    AuditFailedError,
    GrainValidationError,
}


class DirectoryNotFoundError(Exception):
    pass


class FileCopyError(Exception):
    pass


class FileMoveError(Exception):
    pass
