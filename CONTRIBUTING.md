# Contributing

# Setup
1. The repo uses `uv`. You'll want to install `uv` and then you can `uv sync` or utilize the `install` make command.
2. You'll need to install the pre-commits, make sure that you have your environment active in the terminal by utilizing `source .venv/bin/activate`. Then issue the command:  
`uv run -- pre-commit install --install-hooks`
3. Setup any wanted configuration. For development work you need to prefix the environment variables from the `src/settings.py` file with `DEV_` in your `.env` file.

## Adding a New File Type

To add support for a new file type (e.g., TSV, XML, Avro), you need to:

1. **Create a Source class** in `src/sources/base.py`:
   - Extend `DataSource` and add any file-type-specific configuration fields
   - Example: `CSVSource` has `delimiter`, `encoding`, and `skip_rows` fields
   - Example: `ExcelSource` has `sheet_name` and `skip_rows` fields

2. **Create a Reader class** in `src/pipeline/read/`:
   - Extend `BaseReader` from `src/pipeline/read/base.py`
   - Set `SOURCE_TYPE` class variable to your new Source class
   - Implement the `read()` method that:
     - Validates headers exist and match required fields
     - Streams data from the file in batches
     - Yields batches of dictionaries (one dict per row/record)
   - Implement the `starting_row_number` property (for error reporting)
   - Override `_get_file_stream()` if you need custom file handling (e.g., text encoding)
   - See `src/pipeline/read/csv.py` or `src/pipeline/read/excel.py` for examples

3. **Register the Reader** in `src/pipeline/read/factory.py`:
   - Add your file extension(s) to the `_readers` dictionary
   - Map the extension to your Reader class
   - Example: `".tsv": TSVReader` or `".xml": XMLReader`

4. **Update `ReaderFactory.create_reader()`** if needed:
   - Add any new source configuration fields to the `include` set in `reader_kwargs`
   - This ensures those fields are passed to your Reader's `__init__` method

5. **Test your implementation**:
   - Create test files in `src/tests/` 
   - Ensure your Reader handles:
     - Missing headers
     - Missing required columns
     - Empty files
     - Cloud storage (S3, GCS, Azure) if applicable
     - Gzip compression if applicable

6. **Add in Tests**:
   - **Create fixture data** in `src/tests/fixtures/<filetype>_files.py`:
     - Define test data constants as lists/dicts representing file contents
     - Include scenarios: blank headers, duplicates, validation errors, missing columns, failed audits, no data
     - See `src/tests/fixtures/csv_files.py` or `src/tests/fixtures/excel_files.py` for examples
   
   - **Create test source configuration** in `src/tests/fixtures/sources.py`:
     - Define a `TEST_<FILETYPE>_SOURCE` using your new Source class
     - Include a test `TableModel` with appropriate fields
     - Add it to the `test_sources` list in `conftest.py`'s `setup_test_db_and_directories` fixture
   
   - **Create file creation fixture** in `src/tests/conftest.py`:
     - Add a `create_<filetype>_file` fixture function
     - It should accept `file_name` and `data` parameters
     - Create the file in `session_temp_dir` using your file format's library
     - Clean up files after tests (see `create_csv_file` or `create_excel_file` for patterns)
   
   - **Create test file** `src/tests/test_<filetype>.py`:
     - Test blank/missing headers → should raise `MissingHeaderError`
     - Test duplicate grain → should raise `GrainValidationError`
     - Test validation errors exceeding threshold → should raise `ValidationThresholdExceededError`
     - Test validation errors below threshold → should succeed but create DLQ records
     - Test missing required columns → should raise `MissingColumnsError`
     - Test failed audit → should raise `AuditFailedError`
     - Test no data → should raise `NoDataInFileError` (if applicable)
     - Test successful processing → should return `(True, filename, None)`
     - Test DLQ records are created correctly for validation errors
     - Test file-type specific features (e.g., Excel date conversion, JSON array paths)
     - Use the pattern: `test_processor.results.clear()`, process file, assert results
     - See `src/tests/test_csv.py` or `src/tests/test_excel.py` for complete examples
   
   - **Test gzip support** (if applicable):
     - Create `create_<filetype>_gz_file` fixture in `conftest.py`
     - Add test in `src/tests/test_gzip.py` to verify compressed files work
   
   - **Run tests**:
     - Utilize `make test`
     - Tests are pretty fast right now

## Adding a New Database

To add support for a new database (e.g., Oracle, Snowflake, Redshift), you need to:

1. **Update SQLAlchemy connection** in `src/process/db.py`:
   - Ensure SQLAlchemy supports your database (may need additional driver)
   - Update `setup_db()` if needed for database-specific setup
   - Add any required type adapters (see `_register_pendulum_adapters()` for examples)
   - Update `SUPPORTED_DATABASE_DRIVERS` in `src/settings.py` so that the DRIVERNAME is properly extracted from the database url.

2. **Create a Writer class** in `src/pipeline/write/`:
   - Extend `BaseWriter` from `src/pipeline/write/base.py`
   - Override `__init__()` if you need database-specific initialization
   - Override `_convert_record()` if you need database-specific type conversions
   - Override `write()` if you need custom batch insertion logic (e.g., BigQuery uses bulk loading)
   - See `src/pipeline/write/postgresql.py` for a simple example
   - See `src/pipeline/write/bigquery.py` for a complex example with custom write logic

3. **Create an Auditor class** in `src/pipeline/audit/`:
   - Extend `BaseAuditor` from `src/pipeline/audit/base.py`
   - Override `_check_grain_uniqueness()` if your database has different SQL syntax
   - Override `_execute_audit_query()` if your database has different SQL syntax
   - See `src/pipeline/audit/postgresql.py` for a standard example
   - See `src/pipeline/audit/bigquery.py` for an example with custom SQL (FARM_FINGERPRINT)

4. **Create a Publisher class** in `src/pipeline/publish/`:
   - Extend `BasePublisher` from `src/pipeline/publish/base.py`
   - Override `create_publish_sql()` if your database uses different MERGE/UPSERT syntax
   - Override `publish()` if you need custom merge logic
   - See `src/pipeline/publish/postgresql.py` for a standard example
   - See `src/pipeline/publish/bigquery.py` for an example with custom SQL

5. **Register all three classes** in their respective factories:
   - Add to `WriterFactory._writers` in `src/pipeline/write/factory.py`
   - Add to `AuditorFactory._auditors` in `src/pipeline/audit/factory.py`
   - Add to `PublisherFactory._publishers` in `src/pipeline/publish/factory.py`
   - Use the database driver name from `config.DRIVERNAME` as the key

6. **Update database-specific utilities** in `src/pipeline/db_utils.py` if needed:
   - `_get_timezone_aware_datetime_type()` - for datetime column types
   - `db_serialize_json_for_dlq_table()` - for JSON serialization in DLQ
   - `db_start_log()` - for ID generation (BigQuery doesn't support auto-increment)

7. **Update table creation** in `src/process/db.py` if needed:
   - Check `create_tables()` for any database-specific table definitions
   - Update `file_load_dlq` or `file_load_log` table creation if needed

8. **Test your implementation**:
   - Test with sample data files
   - Add in your own Make command in the Makefile for manual dev test runs.
   - Verify:
     - Table creation works correctly
     - Batch inserts work efficiently
     - Grain validation queries work
     - Merge/publish operations work correctly
     - Error handling and DLQ insertion work
