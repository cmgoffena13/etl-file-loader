JSON_DUPLICATES = {
    "entries": {
        "item": [
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
    }
}

JSON_VALIDATION_ERROR = {
    "entries": {
        "item": [
            {
                "entry_id": 1,
                "account_code": "ACC001",
                "account_name": "Cash",
                "debit_amount": "asdf",  # Invalid type (should be float)
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
    }
}

JSON_MISSING_COLUMNS = {
    "entries": {
        "item": [
            {
                "entry_id": 1,
                "account_code": "ACC001",
                "account_name": "Cash",
                "debit_amount": 1000.00,
                "credit_amount": None,
                "description": "Payment received",
                # Missing transaction_date and reference_number
            },
        ]
    }
}

JSON_FAIL_AUDIT = {
    "entries": {
        "item": [
            {
                "entry_id": 1,
                "account_code": "ACC001",
                "account_name": "Cash",
                "debit_amount": -1000.00,  # Negative debit (would fail audit if we had one)
                "credit_amount": None,
                "description": "Payment received",
                "transaction_date": "2024-01-15",
                "reference_number": "REF001",
            },
        ]
    }
}

# JSON file with no data (empty array)
JSON_NO_DATA = {"entries": {"item": []}}
