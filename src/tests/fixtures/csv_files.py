CSV_BLANK_HEADER = [
    ["", "", "", "", "", "", "", ""],
    [
        "TXN001",
        "CUST001",
        "SKU001",
        "2",
        "10.50",
        "21.00",
        "2024-01-15",
        "John Doe",
    ],
]

CSV_DUPLICATES = [
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
        "10.50",
        "21.00",
        "2024-01-15",
        "John Doe",
    ],
    [
        "TXN001",  # Duplicate
        "CUST002",
        "SKU002",
        "1",
        "25.00",
        "25.00",
        "2024-01-16",
        "Jane Smith",
    ],
]

CSV_VALIDATION_ERROR = [
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
        "asdf",
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
