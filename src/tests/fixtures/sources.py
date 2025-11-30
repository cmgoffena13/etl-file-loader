from typing import Optional

from pydantic import Field
from pydantic_extra_types.pendulum_dt import Date, DateTime

from src.sources.base import (
    CSVSource,
    ExcelSource,
    JSONSource,
    ParquetSource,
    TableModel,
)


class TestTransaction(TableModel):
    transaction_id: str
    customer_id: str
    product_sku: str
    quantity: int
    unit_price: float
    total_amount: float
    sale_date: Date
    sales_rep: str


TEST_CSV_SOURCE = CSVSource(
    file_pattern="sales_*.csv",
    source_model=TestTransaction,
    table_name="transactions",
    grain=["transaction_id"],
    delimiter=",",
    encoding="utf-8",
    skip_rows=0,
    validation_error_threshold=0.0,  # Fail on any validation errors
    audit_query="""
        SELECT 
        CASE WHEN 
            SUM(CASE WHEN unit_price > 0 THEN 1 ELSE 0 END) = COUNT(*) 
            THEN 1 ELSE 0 
        END AS unit_price_positive
        FROM {table}
    """,
)


class TestProduct(TableModel):
    sku: str = Field(alias="SKU")
    name: str = Field(alias="Product Name")
    category: str = Field(alias="Category")
    price: float = Field(alias="Price")
    stock_quantity: int = Field(alias="Stock Qty")
    supplier: str = Field(alias="Supplier")
    last_date: Date = Field(alias="Last Date")
    last_updated: DateTime = Field(alias="Last Updated")


TEST_EXCEL_SOURCE = ExcelSource(
    file_pattern="inventory_*.xlsx",
    source_model=TestProduct,
    table_name="products",
    grain=["sku"],
    sheet_name=None,
    skip_rows=0,
    validation_error_threshold=0.0,
    audit_query="""
        SELECT 
        CASE WHEN 
            SUM(CASE WHEN Price > 0 THEN 1 ELSE 0 END) = COUNT(*) 
            THEN 1 ELSE 0 
        END AS unit_price_positive
        FROM {table}
    """,
)

TEST_EXCEL_SOURCE_WITH_SHEET = ExcelSource(
    file_pattern="multi_sheet_*.xlsx",
    source_model=TestProduct,
    table_name="products_sheet2",
    grain=["sku"],
    sheet_name="Sheet2",
    skip_rows=0,
    validation_error_threshold=0.0,
    audit_query="""
        SELECT 
        CASE WHEN 
            SUM(CASE WHEN Price > 0 THEN 1 ELSE 0 END) = COUNT(*) 
            THEN 1 ELSE 0 
        END AS unit_price_positive
        FROM {table}
    """,
)


class TestLedgerEntry(TableModel):
    entry_id: int
    account_code: str
    account_name: str
    debit_amount: Optional[float]
    credit_amount: Optional[float]
    description: str
    transaction_date: Date
    reference_number: str


TEST_JSON_SOURCE = JSONSource(
    file_pattern="ledger_*.json",
    source_model=TestLedgerEntry,
    table_name="ledger_entries",
    grain=["entry_id"],
    array_path="entries.item",
    validation_error_threshold=0.0,
    audit_query="""
        SELECT 
        CASE WHEN 
            SUM(CASE WHEN debit_amount > 0 THEN 1 ELSE 0 END) = COUNT(*) 
            THEN 1 ELSE 0 
        END AS debit_amount_positive
        FROM {table}
    """,
)

TEST_CSV_GZ_SOURCE = CSVSource(
    file_pattern="sales_*.csv.gz",
    source_model=TestTransaction,
    table_name="transactions_gz",
    grain=["transaction_id"],
    delimiter=",",
    encoding="utf-8",
    skip_rows=0,
    validation_error_threshold=0.0,
    audit_query="""
        SELECT 
        CASE WHEN 
            SUM(CASE WHEN unit_price > 0 THEN 1 ELSE 0 END) = COUNT(*) 
            THEN 1 ELSE 0 
        END AS unit_price_positive
        FROM {table}
    """,
)

TEST_JSON_GZ_SOURCE = JSONSource(
    file_pattern="ledger_*.json.gz",
    source_model=TestLedgerEntry,
    table_name="ledger_entries_gz",
    grain=["entry_id"],
    array_path="entries.item",
    validation_error_threshold=0.0,
    audit_query="""
        SELECT 
        CASE WHEN 
            SUM(CASE WHEN debit_amount > 0 THEN 1 ELSE 0 END) = COUNT(*) 
            THEN 1 ELSE 0 
        END AS debit_amount_positive
        FROM {table}
    """,
)

TEST_CSV_SOURCE_WITH_NOTIFICATIONS = CSVSource(
    file_pattern="notify_sales_*.csv",
    source_model=TestTransaction,
    table_name="transactions_notify",
    grain=["transaction_id"],
    delimiter=",",
    encoding="utf-8",
    skip_rows=0,
    validation_error_threshold=0.0,
    audit_query="""
        SELECT 
        CASE WHEN 
            SUM(CASE WHEN unit_price > 0 THEN 1 ELSE 0 END) = COUNT(*) 
            THEN 1 ELSE 0 
        END AS unit_price_positive
        FROM {table}
    """,
    notification_emails=["test@example.com"],
)

# Test source with higher validation error threshold (allows some errors)
TEST_CSV_SOURCE_WITH_THRESHOLD = CSVSource(
    file_pattern="threshold_sales_*.csv",
    source_model=TestTransaction,
    table_name="transactions_threshold",
    grain=["transaction_id"],
    delimiter=",",
    encoding="utf-8",
    skip_rows=0,
    validation_error_threshold=0.15,  # Allow up to 15% error rate
    audit_query="""
        SELECT 
        CASE WHEN 
            SUM(CASE WHEN unit_price > 0 THEN 1 ELSE 0 END) = COUNT(*) 
            THEN 1 ELSE 0 
        END AS unit_price_positive
        FROM {table}
    """,
)


TEST_PARQUET_SOURCE = ParquetSource(
    file_pattern="ledger_*.parquet",
    source_model=TestLedgerEntry,
    table_name="ledger_entries_parquet",
    grain=["entry_id"],
    validation_error_threshold=0.0,
    audit_query="""
        SELECT 
        CASE WHEN 
            SUM(CASE WHEN debit_amount > 0 THEN 1 ELSE 0 END) = COUNT(*) 
            THEN 1 ELSE 0 
        END AS debit_amount_positive
        FROM {table}
    """,
)
