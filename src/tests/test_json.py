from src.tests.fixtures.json_files import (
    JSON_DUPLICATES,
    JSON_FAIL_AUDIT,
    JSON_MISSING_COLUMNS,
    JSON_NO_DATA,
    JSON_VALIDATION_ERROR,
)


def test_json_with_duplicate_grain(create_json_file, test_processor):
    """Test that JSON files with duplicate grain values are caught during audit."""
    create_json_file("ledger_duplicates.json", JSON_DUPLICATES)

    # Clear results from previous tests
    test_processor.results.clear()

    # Process the file - should fail during grain audit
    test_processor.process_file("ledger_duplicates.json")

    # Check that processing failed due to duplicate grain
    assert len(test_processor.results) == 1
    success, _, error = test_processor.results[0]
    assert success is False
    assert error == "GrainValidationError"


def test_json_with_validation_error(create_json_file, test_processor):
    """Test that JSON files with validation errors exceeding threshold result in failure."""
    create_json_file("ledger_validation_error.json", JSON_VALIDATION_ERROR)

    # Clear results from previous tests
    test_processor.results.clear()

    # Process the file - should fail due to validation threshold exceeded
    test_processor.process_file("ledger_validation_error.json")

    # Check that processing failed due to validation threshold
    assert len(test_processor.results) == 1
    success, filename, error = test_processor.results[0]
    assert success is False
    assert "ledger_validation_error.json" in filename
    assert error == "ValidationThresholdExceededError"


def test_json_with_missing_columns(create_json_file, test_processor):
    """Test that JSON files with missing required columns raise appropriate errors."""
    create_json_file("ledger_missing_columns.json", JSON_MISSING_COLUMNS)

    test_processor.results.clear()

    # Process the file - should fail due to missing columns
    test_processor.process_file("ledger_missing_columns.json")

    # Check that processing failed
    assert len(test_processor.results) == 1
    success, filename, error = test_processor.results[0]
    assert success is False
    assert "ledger_missing_columns.json" in filename
    assert error == "MissingColumnsError"


def test_json_with_fail_audit(create_json_file, test_processor):
    """Test that JSON files that fail audit raise appropriate errors."""
    create_json_file("ledger_fail_audit.json", JSON_FAIL_AUDIT)

    test_processor.results.clear()

    # Process the file - should fail during audit
    test_processor.process_file("ledger_fail_audit.json")

    # Check that processing failed due to audit failure
    assert len(test_processor.results) == 1
    success, filename, error = test_processor.results[0]
    assert success is False
    assert "ledger_fail_audit.json" in filename
    assert error == "AuditFailedError"


def test_json_with_no_data(create_json_file, test_processor):
    """Test that JSON files with no data raise NoDataInFileError."""
    create_json_file("ledger_no_data.json", JSON_NO_DATA)

    test_processor.results.clear()

    # Process the file - should fail due to no data
    test_processor.process_file("ledger_no_data.json")

    # Check that processing failed
    assert len(test_processor.results) == 1
    success, filename, error = test_processor.results[0]
    assert success is False
    assert "ledger_no_data.json" in filename
    assert error == "NoDataInFileError"
