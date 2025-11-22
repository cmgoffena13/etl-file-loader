from pathlib import Path

from sqlalchemy import Engine

from src.pipeline.audit.base import BaseAuditor
from src.sources.base import DataSource


class SQLiteAuditor(BaseAuditor):
    def __init__(
        self,
        file_path: Path,
        source: DataSource,
        engine: Engine,
        stage_table_name: str,
        log_id: int,
    ):
        super().__init__(file_path, source, engine, stage_table_name, log_id)

    def create_grain_validation_sql(self):
        grain_sql = None
        if len(self.source.grain) == 1:
            grain_sql = f"SELECT CASE WHEN COUNT(DISTINCT {self.source.grain[0]}) = COUNT(*) THEN 1 ELSE 0 END AS grain_unique FROM {{table}}"
        else:
            concat_args = []
            for col in self.source.grain:
                concat_args.append(col)
                concat_args.append("'||'")
            concat_args = concat_args[:-1]
            concat_expr = "CONCAT(" + ", ".join(concat_args) + ")"
            grain_sql = f"SELECT CASE WHEN COUNT(DISTINCT {concat_expr}) = COUNT(*) THEN 1 ELSE 0 END AS grain_unique FROM {{table}}"
        return grain_sql
