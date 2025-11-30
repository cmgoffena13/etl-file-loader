from sqlalchemy import select, text

from src.tests.fixtures.excel_files import (
    EXCEL_BLANK_HEADER,
    EXCEL_DUPLICATES,
    EXCEL_EPOCH_CONVERSION,
    EXCEL_FAIL_AUDIT,
    EXCEL_MISSING_COLUMNS,
    EXCEL_NO_DATA,
    EXCEL_SHEET1_DATA,
    EXCEL_SHEET2_DATA,
    EXCEL_VALIDATION_ERROR,
)


def test_excel_with_blank_header(create_excel_file, test_processor):
    """Test that Excel files with blank headers raise appropriate errors."""
    create_excel_file("inventory_blank_header.xlsx", EXCEL_BLANK_HEADER)

    test_processor.results.clear()
    test_processor.process_file("inventory_blank_header.xlsx")

    # Check that processing failed
    assert len(test_processor.results) == 1
    success, filename, error = test_processor.results[0]
    assert success is False
    assert "inventory_blank_header.xlsx" in filename
    assert error == "MissingHeaderError"


def test_excel_with_duplicate_grain(create_excel_file, test_processor):
    """Test that Excel files with duplicate grain values are caught during audit."""
    create_excel_file("inventory_duplicates.xlsx", EXCEL_DUPLICATES)

    # Clear results from previous tests
    test_processor.results.clear()

    # Process the file - should fail during grain audit
    test_processor.process_file("inventory_duplicates.xlsx")

    # Check that processing failed due to duplicate grain
    assert len(test_processor.results) == 1
    success, _, error = test_processor.results[0]
    assert success is False
    assert error == "GrainValidationError"


def test_excel_with_validation_error(create_excel_file, test_processor):
    """Test that Excel files with validation errors exceeding threshold result in failure."""
    create_excel_file("inventory_validation_error.xlsx", EXCEL_VALIDATION_ERROR)

    # Clear results from previous tests
    test_processor.results.clear()

    # Process the file - should fail due to validation threshold exceeded
    test_processor.process_file("inventory_validation_error.xlsx")

    # Check that processing failed due to validation threshold
    assert len(test_processor.results) == 1
    success, filename, error = test_processor.results[0]
    assert success is False
    assert "inventory_validation_error.xlsx" in filename
    assert error == "ValidationThresholdExceededError"

    # Verify that validation errors were inserted into DLQ table
    with test_processor.engine.connect() as conn:
        dlq_table = test_processor.file_load_dlq_table
        result = conn.execute(
            select(dlq_table).where(
                dlq_table.c.source_filename == "inventory_validation_error.xlsx"
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
        assert first_record.source_filename == "inventory_validation_error.xlsx"
        assert first_record.validation_errors is not None
        assert first_record.file_record_data is not None


def test_excel_with_missing_header(create_excel_file, test_processor):
    """Test that Excel files with missing headers raise appropriate errors."""
    # Create file with blank header row (no valid headers)
    create_excel_file("inventory_no_header.xlsx", EXCEL_BLANK_HEADER)

    test_processor.results.clear()

    # Process the file - should fail due to missing header
    test_processor.process_file("inventory_no_header.xlsx")

    # Check that processing failed
    assert len(test_processor.results) == 1
    success, filename, error = test_processor.results[0]
    assert success is False
    assert "inventory_no_header.xlsx" in filename
    assert error == "MissingHeaderError"


def test_excel_with_missing_columns(create_excel_file, test_processor):
    """Test that Excel files with missing required columns raise appropriate errors."""
    create_excel_file("inventory_missing_columns.xlsx", EXCEL_MISSING_COLUMNS)

    test_processor.results.clear()

    # Process the file - should fail due to missing columns
    test_processor.process_file("inventory_missing_columns.xlsx")

    # Check that processing failed
    assert len(test_processor.results) == 1
    success, filename, error = test_processor.results[0]
    assert success is False
    assert "inventory_missing_columns.xlsx" in filename
    assert error == "MissingColumnsError"


def test_excel_with_fail_audit(create_excel_file, test_processor):
    """Test that Excel files that fail audit raise appropriate errors."""
    create_excel_file("inventory_fail_audit.xlsx", EXCEL_FAIL_AUDIT)

    test_processor.results.clear()

    # Process the file - should fail during audit
    test_processor.process_file("inventory_fail_audit.xlsx")

    # Check that processing failed due to audit failure
    assert len(test_processor.results) == 1
    success, filename, error = test_processor.results[0]
    assert success is False
    assert "inventory_fail_audit.xlsx" in filename
    assert error == "AuditFailedError"


def test_excel_epoch_conversion(create_excel_file, test_processor):
    """Test that Excel date serial numbers are correctly converted to dates/datetimes."""
    create_excel_file("inventory_epoch_conversion.xlsx", EXCEL_EPOCH_CONVERSION)

    test_processor.results.clear()

    # Process the file - should succeed with proper date conversion
    # If epoch conversion fails, Pydantic validation will fail and processing will fail
    test_processor.process_file("inventory_epoch_conversion.xlsx")

    # Check that processing succeeded (validates that epoch conversion worked)
    assert len(test_processor.results) == 1
    success, filename, error = test_processor.results[0]
    assert success is True
    assert "inventory_epoch_conversion.xlsx" in filename
    assert error is None


def test_excel_with_no_data(create_excel_file, test_processor):
    """Test that Excel files with no data raise NoDataInFileError."""
    create_excel_file("inventory_no_data.xlsx", EXCEL_NO_DATA)

    test_processor.results.clear()

    # Process the file - should fail due to no data
    test_processor.process_file("inventory_no_data.xlsx")

    # Check that processing failed
    assert len(test_processor.results) == 1
    success, filename, error = test_processor.results[0]
    assert success is False
    assert "inventory_no_data.xlsx" in filename
    assert error == "NoDataInFileError"


def test_excel_with_specific_sheet_name(create_multi_sheet_excel_file, test_processor):
    """Test that Excel files with a specific sheet_name read from the correct tab."""
    # Create Excel file with multiple sheets
    create_multi_sheet_excel_file(
        "multi_sheet_test.xlsx",
        {"Sheet1": EXCEL_SHEET1_DATA, "Sheet2": EXCEL_SHEET2_DATA},
    )

    test_processor.results.clear()

    # Process the file - should read from Sheet2 (as specified in TEST_EXCEL_SOURCE_WITH_SHEET)
    test_processor.process_file("multi_sheet_test.xlsx")

    # Check that processing succeeded
    assert len(test_processor.results) == 1
    success, filename, error = test_processor.results[0]
    assert success is True
    assert "multi_sheet_test.xlsx" in filename
    assert error is None

    # Verify that the correct sheet data was read (Sheet2 data, not Sheet1)
    with test_processor.engine.connect() as conn:
        result = conn.execute(text("SELECT sku FROM products_sheet2")).fetchall()

        # Should have 2 records from Sheet2
        assert len(result) == 2

        # Verify the SKUs are from Sheet2 (SKU003 and SKU004), not Sheet1 (SKU001 and SKU002)
        skus = [row[0] for row in result]
        assert "SKU003" in skus
        assert "SKU004" in skus
        assert "SKU001" not in skus
        assert "SKU002" not in skus
