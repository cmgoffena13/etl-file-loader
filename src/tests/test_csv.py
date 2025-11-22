from src.tests.fixtures.csv_files import (
    CSV_BLANK_HEADER,
    CSV_DUPLICATES,
    CSV_VALIDATION_ERROR,
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
    assert error is not None


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
    assert error is not None


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
    assert error is not None
