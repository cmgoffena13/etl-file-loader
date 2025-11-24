from pathlib import Path
from typing import Union

from sqlalchemy import Engine

from src.pipeline.audit.base import BaseAuditor
from src.sources.base import DataSource


class BigQueryAuditor(BaseAuditor):
    def __init__(
        self,
        file_path: Union[Path, str],
        source: DataSource,
        engine: Engine,
        stage_table_name: str,
        log_id: int,
    ):
        super().__init__(file_path, source, engine, stage_table_name, log_id)

    def create_grain_validation_sql(self) -> str:
        if len(self.source.grain) == 1:
            grain_sql = f"SELECT CASE WHEN COUNT(DISTINCT {self.source.grain[0]}) = COUNT(*) THEN 1 ELSE 0 END AS grain_unique FROM {{table}}"
        else:
            concat_parts = []
            for col in self.source.grain:
                concat_parts.append(f"CAST({col} AS STRING)")
            concat_expr = ", ".join(concat_parts)
            grain_sql = f"SELECT CASE WHEN COUNT(DISTINCT FARM_FINGERPRINT(CONCAT({concat_expr}))) = COUNT(*) THEN 1 ELSE 0 END AS grain_unique FROM {{table}}"
        return grain_sql
