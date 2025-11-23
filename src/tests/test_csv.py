from sqlalchemy import select

from src.tests.fixtures.csv_files import (
    CSV_BLANK_HEADER,
    CSV_DUPLICATES,
    CSV_FAIL_AUDIT,
    CSV_MISSING_COLUMNS,
    CSV_VALIDATION_ERROR,
    CSV_VALIDATION_ERRORS_BELOW_THRESHOLD,
)


def test_csv_with_blank_header(create_csv_file, test_processor):
    """Test that CSV files with blank headers raise appropriate errors."""
    create_csv_file("sales_blank_header.csv", CSV_BLANK_HEADER)

    test_processor.results.clear()
    test_processor.process_file("sales_blank_header.csv")

    # Check that processing failed
    assert len(test_processor.results) == 1
    success, filename, error = test_processor.results[0]
    assert success is False
    assert "sales_blank_header.csv" in filename
    assert error == "MissingHeaderError"


def test_csv_with_duplicate_grain(create_csv_file, test_processor):
    """Test that CSV files with duplicate grain values are caught during audit."""
    create_csv_file("sales_duplicates.csv", CSV_DUPLICATES)

    # Clear results from previous tests
    test_processor.results.clear()

    # Process the file - should fail during grain audit
    test_processor.process_file("sales_duplicates.csv")

    # Check that processing failed due to duplicate grain
    assert len(test_processor.results) == 1
    success, _, error = test_processor.results[0]
    assert success is False
    assert error == "GrainValidationError"


def test_csv_with_validation_error(create_csv_file, test_processor):
    """Test that CSV files with validation errors exceeding threshold result in failure."""
    create_csv_file("sales_validation_error.csv", CSV_VALIDATION_ERROR)

    # Clear results from previous tests
    test_processor.results.clear()

    # Process the file - should fail due to validation threshold exceeded
    test_processor.process_file("sales_validation_error.csv")

    # Check that processing failed due to validation threshold
    assert len(test_processor.results) == 1
    success, filename, error = test_processor.results[0]
    assert success is False
    assert "sales_validation_error.csv" in filename
    assert error == "ValidationThresholdExceededError"

    # Verify that validation errors were inserted into DLQ table
    with test_processor.engine.connect() as conn:
        dlq_table = test_processor.file_load_dlq_table
        result = conn.execute(
            select(dlq_table).where(
                dlq_table.c.source_filename == "sales_validation_error.csv"
            )
        ).fetchall()
        assert len(result) > 0, (
            "Expected validation errors to be inserted into DLQ table"
        )
        # Verify the first record has validation errors
        first_record = result[0]
        assert (
            first_record.file_row_number == 2
        )  # Second row (after header) has the error
        assert first_record.source_filename == "sales_validation_error.csv"
        assert first_record.validation_errors is not None
        assert first_record.file_record_data is not None


def test_csv_with_missing_header(create_csv_file, test_processor):
    """Test that CSV files with missing headers raise appropriate errors."""
    create_csv_file("sales_no_header.csv", [])

    test_processor.results.clear()

    # Process the file - should fail due to missing header
    test_processor.process_file("sales_no_header.csv")

    # Check that processing failed
    assert len(test_processor.results) == 1
    success, filename, error = test_processor.results[0]
    assert success is False
    assert "sales_no_header.csv" in filename
    assert error == "MissingHeaderError"


def test_csv_with_validation_errors_below_threshold(create_csv_file, test_processor):
    """Test that CSV files with validation errors below threshold succeed."""
    create_csv_file(
        "threshold_sales_validation.csv", CSV_VALIDATION_ERRORS_BELOW_THRESHOLD
    )

    test_processor.results.clear()

    # Process the file - should succeed because error rate (1/10 = 0.1) is below threshold (0.15)
    test_processor.process_file("threshold_sales_validation.csv")

    # Check that processing succeeded
    assert len(test_processor.results) == 1
    success, filename, error = test_processor.results[0]
    assert success is True
    assert "threshold_sales_validation.csv" in filename
    assert error is None

    # Verify that validation errors were still inserted into DLQ table
    with test_processor.engine.connect() as conn:
        dlq_table = test_processor.file_load_dlq_table
        result = conn.execute(
            select(dlq_table).where(
                dlq_table.c.source_filename == "threshold_sales_validation.csv"
            )
        ).fetchall()
        assert len(result) > 0, (
            "Expected validation errors to be inserted into DLQ table"
        )
        # Should have 1 DLQ record for the invalid price
        assert len(result) == 1


def test_csv_with_missing_columns(create_csv_file, test_processor):
    """Test that CSV files with missing required columns raise appropriate errors."""
    create_csv_file("sales_missing_columns.csv", CSV_MISSING_COLUMNS)

    test_processor.results.clear()

    # Process the file - should fail due to missing columns
    test_processor.process_file("sales_missing_columns.csv")

    # Check that processing failed
    assert len(test_processor.results) == 1
    success, filename, error = test_processor.results[0]
    assert success is False
    assert "sales_missing_columns.csv" in filename
    assert error == "MissingColumnsError"


def test_csv_with_fail_audit(create_csv_file, test_processor):
    """Test that CSV files that fail audit raise appropriate errors."""
    create_csv_file("sales_fail_audit.csv", CSV_FAIL_AUDIT)

    test_processor.results.clear()

    # Process the file - should fail during audit
    test_processor.process_file("sales_fail_audit.csv")

    # Check that processing failed due to audit failure
    assert len(test_processor.results) == 1
    success, filename, error = test_processor.results[0]
    assert success is False
    assert "sales_fail_audit.csv" in filename
    assert error == "AuditFailedError"


def test_csv_dlq_records_deleted_on_reprocess(create_csv_file, test_processor):
    """Test that DLQ records are deleted when a file is reprocessed successfully."""
    # First, process file with validation errors (creates DLQ records)
    create_csv_file("sales_reprocess_test.csv", CSV_VALIDATION_ERROR)

    test_processor.results.clear()
    test_processor.process_file("sales_reprocess_test.csv")

    # Verify processing failed and DLQ records were created
    assert len(test_processor.results) == 1
    success, filename, error = test_processor.results[0]
    assert success is False
    assert error == "ValidationThresholdExceededError"

    # Verify DLQ records exist
    with test_processor.engine.connect() as conn:
        dlq_table = test_processor.file_load_dlq_table
        result = conn.execute(
            select(dlq_table).where(
                dlq_table.c.source_filename == "sales_reprocess_test.csv"
            )
        ).fetchall()
        assert len(result) > 0, "Expected DLQ records to exist after validation failure"
        initial_dlq_count = len(result)

    # Create a fixed version of the file (without validation errors)
    CSV_FIXED = [
        [
            "transaction_id",
            "customer_id",
            "product_sku",
            "quantity",
            "unit_price",
            "total_amount",
            "sale_date",
            "sales_rep",
        ],
        [
            "TXN001",
            "CUST001",
            "SKU001",
            "2",
            "10.50",  # Fixed: was "asdf"
            "21.00",
            "2024-01-15",
            "John Doe",
        ],
        [
            "TXN002",
            "CUST002",
            "SKU002",
            "1",
            "25.00",
            "25.00",
            "2024-01-16",
            "Jane Smith",
        ],
    ]
    create_csv_file("sales_reprocess_test.csv", CSV_FIXED)

    # Reprocess the file - should succeed
    test_processor.results.clear()
    test_processor.process_file("sales_reprocess_test.csv")

    # Verify processing succeeded
    assert len(test_processor.results) == 1
    success, filename, error = test_processor.results[0]
    assert success is True
    assert "sales_reprocess_test.csv" in filename
    assert error is None

    # Verify old DLQ records were deleted
    with test_processor.engine.connect() as conn:
        dlq_table = test_processor.file_load_dlq_table
        result = conn.execute(
            select(dlq_table).where(
                dlq_table.c.source_filename == "sales_reprocess_test.csv"
            )
        ).fetchall()
        assert len(result) == 0, (
            f"Expected DLQ records to be deleted after successful reprocess, but found {len(result)} records"
        )
