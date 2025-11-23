from typing import Optional

from pydantic import Field
from pydantic_extra_types.pendulum_dt import Date

from src.sources.base import ParquetSource, TableModel


class LedgerEntryParquet(TableModel):
    entry_id: int
    account_code: str = Field(max_length=100)
    account_name: str = Field(max_length=100)
    debit_amount: Optional[float]
    credit_amount: Optional[float]
    description: str = Field(max_length=500)
    transaction_date: Date
    reference_number: str = Field(max_length=100)


FINANCIAL_PARQUET = ParquetSource(
    file_pattern="ledger_*.parquet",
    source_model=LedgerEntryParquet,
    table_name="financial_ledger_parquet",
    grain=["entry_id"],
)
