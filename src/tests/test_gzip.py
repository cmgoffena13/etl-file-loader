from src.tests.fixtures.csv_files import CSV_DUPLICATES
from src.tests.fixtures.json_files import JSON_DUPLICATES


def test_csv_gz_file_reading(create_csv_gz_file, test_processor):
    """Test that gzipped CSV files can be read and processed."""
    create_csv_gz_file("sales_2024.csv.gz", CSV_DUPLICATES)

    test_processor.results.clear()

    # Process the file - should succeed (duplicates will be caught in audit)
    test_processor.process_file("sales_2024.csv.gz")

    # Check that processing failed due to duplicate grain
    assert len(test_processor.results) == 1
    success, filename, error = test_processor.results[0]
    assert success is False
    assert "sales_2024.csv.gz" in filename
    assert error == "GrainValidationError"


def test_json_gz_file_reading(create_json_gz_file, test_processor):
    """Test that gzipped JSON files can be read and processed."""
    create_json_gz_file("ledger_2024.json.gz", JSON_DUPLICATES)

    test_processor.results.clear()

    # Process the file - should succeed (duplicates will be caught in audit)
    test_processor.process_file("ledger_2024.json.gz")

    # Check that processing failed due to duplicate grain
    assert len(test_processor.results) == 1
    success, filename, error = test_processor.results[0]
    assert success is False
    assert "ledger_2024.json.gz" in filename
    assert error == "GrainValidationError"
