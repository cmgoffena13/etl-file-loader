import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import select

from src.tests.fixtures.parquet_files import (
    PARQUET_DUPLICATES,
    PARQUET_FAIL_AUDIT,
    PARQUET_MISSING_COLUMNS,
    PARQUET_NO_DATA,
    PARQUET_VALIDATION_ERROR,
)


def test_parquet_with_duplicate_grain(create_parquet_file, test_processor):
    """Test that Parquet files with duplicate grain values are caught during audit."""
    create_parquet_file("ledger_duplicates.parquet", PARQUET_DUPLICATES)

    test_processor.results.clear()

    # Process the file - should fail during grain audit
    test_processor.process_file("ledger_duplicates.parquet")

    # Check that processing failed due to duplicate grain
    assert len(test_processor.results) == 1
    success, _, error = test_processor.results[0]
    assert success is False
    assert error == "GrainValidationError"


def test_parquet_with_validation_error(create_parquet_file, test_processor):
    """Test that Parquet files with validation errors exceeding threshold result in failure."""
    create_parquet_file("ledger_validation_error.parquet", PARQUET_VALIDATION_ERROR)

    test_processor.results.clear()

    # Process the file - should fail due to validation threshold exceeded
    test_processor.process_file("ledger_validation_error.parquet")

    # Check that processing failed due to validation threshold
    assert len(test_processor.results) == 1
    success, filename, error = test_processor.results[0]
    assert success is False
    assert "ledger_validation_error.parquet" in filename
    assert error == "ValidationThresholdExceededError"

    # Verify that validation errors were inserted into DLQ table
    with test_processor.engine.connect() as conn:
        dlq_table = test_processor.file_load_dlq_table
        result = conn.execute(
            select(dlq_table).where(
                dlq_table.c.source_filename == "ledger_validation_error.parquet"
            )
        ).fetchall()
        assert len(result) > 0, (
            "Expected validation errors to be inserted into DLQ table"
        )
        # Verify the first record has validation errors
        first_record = result[0]
        assert first_record.file_row_number == 1  # First row in parquet file
        assert first_record.source_filename == "ledger_validation_error.parquet"
        assert first_record.validation_errors is not None
        assert first_record.file_record_data is not None


def test_parquet_with_missing_columns(create_parquet_file, test_processor):
    """Test that Parquet files with missing required columns raise appropriate errors."""
    create_parquet_file("ledger_missing_columns.parquet", PARQUET_MISSING_COLUMNS)

    test_processor.results.clear()

    # Process the file - should fail due to missing columns
    test_processor.process_file("ledger_missing_columns.parquet")

    # Check that processing failed
    assert len(test_processor.results) == 1
    success, filename, error = test_processor.results[0]
    assert success is False
    assert "ledger_missing_columns.parquet" in filename
    assert error == "MissingColumnsError"


def test_parquet_with_fail_audit(create_parquet_file, test_processor):
    """Test that Parquet files that fail audit raise appropriate errors."""
    create_parquet_file("ledger_fail_audit.parquet", PARQUET_FAIL_AUDIT)

    test_processor.results.clear()

    # Process the file - should fail during audit
    test_processor.process_file("ledger_fail_audit.parquet")

    # Check that processing failed due to audit failure
    assert len(test_processor.results) == 1
    success, filename, error = test_processor.results[0]
    assert success is False
    assert "ledger_fail_audit.parquet" in filename
    assert error == "AuditFailedError"


def test_parquet_with_no_data(session_temp_dir, test_processor):
    """Test that Parquet files with no data raise NoDataInFileError."""
    # Create an empty parquet file with the expected schema
    schema = pa.schema(
        [
            ("entry_id", pa.int64()),
            ("account_code", pa.string()),
            ("account_name", pa.string()),
            ("debit_amount", pa.float64()),
            ("credit_amount", pa.float64()),
            ("description", pa.string()),
            ("transaction_date", pa.date32()),
            ("reference_number", pa.string()),
        ]
    )
    empty_table = pa.Table.from_pylist(PARQUET_NO_DATA, schema=schema)
    file_path = session_temp_dir / "ledger_no_data.parquet"
    pq.write_table(empty_table, file_path)

    test_processor.results.clear()

    # Process the file - should fail due to no data
    test_processor.process_file("ledger_no_data.parquet")

    # Check that processing failed
    assert len(test_processor.results) == 1
    success, filename, error = test_processor.results[0]
    assert success is False
    assert "ledger_no_data.parquet" in filename
    assert error == "NoDataInFileError"
