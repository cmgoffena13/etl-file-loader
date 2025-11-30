EXCEL_BLANK_HEADER = [
    ["", "", "", "", "", "", "", ""],
    [
        "SKU001",
        "Product A",
        "Category 1",
        "10.50",
        "100",
        "Supplier A",
        "2024-01-15",
        "2024-01-15T10:00:00",
    ],
]

EXCEL_DUPLICATES = [
    [
        "SKU",
        "Product Name",
        "Category",
        "Price",
        "Stock Qty",
        "Supplier",
        "Last Date",
        "Last Updated",
    ],
    [
        "SKU001",
        "Product A",
        "Category 1",
        "10.50",
        "100",
        "Supplier A",
        "2024-01-15",
        "2024-01-15T10:00:00",
    ],
    [
        "SKU001",  # Duplicate
        "Product B",
        "Category 2",
        "25.00",
        "50",
        "Supplier B",
        "2024-01-16",
        "2024-01-16T10:00:00",
    ],
]

EXCEL_VALIDATION_ERROR = [
    [
        "SKU",
        "Product Name",
        "Category",
        "Price",
        "Stock Qty",
        "Supplier",
        "Last Date",
        "Last Updated",
    ],
    [
        "SKU001",
        "Product A",
        "Category 1",
        "asdf",  # Invalid price
        "100",
        "Supplier A",
        "2024-01-15",
        "2024-01-15T10:00:00",
    ],
    [
        "SKU002",
        "Product B",
        "Category 2",
        "25.00",
        "50",
        "Supplier B",
        "2024-01-16",
        "2024-01-16T10:00:00",
    ],
]

EXCEL_MISSING_COLUMNS = [
    [
        "SKU",
        "Product Name",
        "Category",
        "Price",
        "Stock Qty",
        "Supplier",
        "Last Updated",
    ],
    [
        "SKU001",
        "Product A",
        "Category 1",
        "10.50",
        "100",
        "Supplier A",
        "2024-01-15T10:00:00",
    ],
]

EXCEL_FAIL_AUDIT = [
    [
        "SKU",
        "Product Name",
        "Category",
        "Price",
        "Stock Qty",
        "Supplier",
        "Last Date",
        "Last Updated",
    ],
    [
        "SKU001",
        "Product A",
        "Category 1",
        "-10.50",  # Negative price (would fail audit if we had one)
        "100",
        "Supplier A",
        "2024-01-15",
        "2024-01-15T10:00:00",
    ],
]

# Excel file with only headers, no data rows (triggers NoDataInFileError)
EXCEL_NO_DATA = [
    [
        "SKU",
        "Product Name",
        "Category",
        "Price",
        "Stock Qty",
        "Supplier",
        "Last Date",
        "Last Updated",
    ],
]

# Excel date serial numbers for epoch conversion testing
# Serial 1 = 1900-01-01 (Excel epoch is 1899-12-30)
# Serial 45321 = 2024-01-15 (approximately)
# Serial 45321.5 = 2024-01-15 12:00:00 (with time component)
EXCEL_EPOCH_CONVERSION = [
    [
        "SKU",
        "Product Name",
        "Category",
        "Price",
        "Stock Qty",
        "Supplier",
        "Last Date",
        "Last Updated",
    ],
    [
        "SKU001",
        "Product A",
        "Category 1",
        "10.50",
        "100",
        "Supplier A",
        1,  # Excel serial number for 1900-01-01 (Date)
        1.5,  # Excel serial number for 1900-01-01 12:00:00 (DateTime)
    ],
    [
        "SKU002",
        "Product B",
        "Category 2",
        "25.00",
        "50",
        "Supplier B",
        45321,  # Excel serial number for 2024-01-15 (Date)
        45321.5,  # Excel serial number for 2024-01-15 12:00:00 (DateTime)
    ],
]

# Test data for multi-sheet Excel file - Sheet1
EXCEL_SHEET1_DATA = [
    [
        "SKU",
        "Product Name",
        "Category",
        "Price",
        "Stock Qty",
        "Supplier",
        "Last Date",
        "Last Updated",
    ],
    [
        "SKU001",
        "Product A",
        "Category 1",
        "10.50",
        "100",
        "Supplier A",
        "2024-01-15",
        "2024-01-15T10:00:00",
    ],
    [
        "SKU002",
        "Product B",
        "Category 2",
        "25.00",
        "50",
        "Supplier B",
        "2024-01-16",
        "2024-01-16T10:00:00",
    ],
]

# Test data for multi-sheet Excel file - Sheet2 (different data)
EXCEL_SHEET2_DATA = [
    [
        "SKU",
        "Product Name",
        "Category",
        "Price",
        "Stock Qty",
        "Supplier",
        "Last Date",
        "Last Updated",
    ],
    [
        "SKU003",
        "Product C",
        "Category 3",
        "15.75",
        "200",
        "Supplier C",
        "2024-01-17",
        "2024-01-17T10:00:00",
    ],
    [
        "SKU004",
        "Product D",
        "Category 4",
        "30.00",
        "75",
        "Supplier D",
        "2024-01-18",
        "2024-01-18T10:00:00",
    ],
]
