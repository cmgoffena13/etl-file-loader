PARQUET_DUPLICATES = [
    {
        "entry_id": 1,
        "account_code": "ACC001",
        "account_name": "Cash",
        "debit_amount": 1000.00,
        "credit_amount": None,
        "description": "Payment received",
        "transaction_date": "2024-01-15",
        "reference_number": "REF001",
    },
    {
        "entry_id": 1,  # Duplicate entry_id
        "account_code": "ACC002",
        "account_name": "Revenue",
        "debit_amount": None,
        "credit_amount": 1000.00,
        "description": "Sale made",
        "transaction_date": "2024-01-15",
        "reference_number": "REF001",
    },
]

PARQUET_VALIDATION_ERROR = [
    {
        "entry_id": 1,
        "account_code": "ACC001",
        "account_name": "Cash",
        "debit_amount": "invalid",  # Invalid type - should be float or None
        "credit_amount": None,
        "description": "Payment received",
        "transaction_date": "2024-01-15",
        "reference_number": "REF001",
    },
    {
        "entry_id": 2,
        "account_code": "ACC002",
        "account_name": "Revenue",
        "debit_amount": None,
        "credit_amount": 1000.00,
        "description": "Sale made",
        "transaction_date": "2024-01-16",
        "reference_number": "REF002",
    },
]

PARQUET_MISSING_COLUMNS = [
    {
        "entry_id": 1,
        "account_code": "ACC001",
        "account_name": "Cash",
        "debit_amount": 1000.00,
        "credit_amount": None,
        # Missing: description, transaction_date, reference_number
    },
]

PARQUET_FAIL_AUDIT = [
    {
        "entry_id": 1,
        "account_code": "ACC001",
        "account_name": "Cash",
        "debit_amount": -1000.00,  # Negative debit (should fail audit)
        "credit_amount": None,
        "description": "Payment received",
        "transaction_date": "2024-01-15",
        "reference_number": "REF001",
    },
]

PARQUET_NO_DATA = []
