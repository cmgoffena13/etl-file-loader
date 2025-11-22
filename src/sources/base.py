from pathlib import Path
from typing import Optional, Type

from pydantic import BaseModel, ConfigDict, Field, model_validator


class TableModel(BaseModel):
    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)


class DataSource(BaseModel):
    file_pattern: str
    source_model: Type[TableModel]
    table_name: str
    grain: list[str]
    audit_query: Optional[str] = None
    validation_error_threshold: float = Field(default=0.0)
    notification_emails: Optional[list[str]] = None

    @model_validator(mode="after")
    def validate_grain_fields(self):
        model_fields = set(self.source_model.model_fields.keys())
        invalid_grain = [g for g in self.grain if g not in model_fields]
        if invalid_grain:
            raise ValueError(
                f"Grain columns {invalid_grain} are not fields in {self.source_model.__name__}. "
                f"Available fields: {sorted(model_fields)}"
            )
        return self

    def matches_file(self, file_path: str) -> bool:
        """Match file path against pattern. Handles both local paths and URI strings."""
        if "/" in file_path and not file_path.startswith("/") and "://" in file_path:
            filename = file_path.split("/")[-1].split("?")[0].split("#")[0]
        else:
            filename = Path(file_path).name

        return Path(filename.lower()).match(self.file_pattern.lower())


class CSVSource(DataSource):
    delimiter: str = Field(default=",")
    encoding: str = Field(default="utf-8")
    skip_rows: int = Field(default=0)


class ExcelSource(DataSource):
    sheet_name: Optional[str] = None
    skip_rows: int = Field(default=0)


class JSONSource(DataSource):
    array_path: str = Field(default="item")
