import json
import logging
import re
from decimal import Decimal
from pathlib import Path
from sqlite3 import register_adapter
from typing import Any, Dict, Union, get_args, get_origin

import pendulum
import pymysql.converters
import xxhash
from annotated_types import MaxLen
from pydantic import EmailStr
from pydantic_extra_types.pendulum_dt import Date, DateTime
from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    Engine,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    MetaData,
    Numeric,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    insert,
    text,
)
from sqlalchemy import Date as SQLDate
from sqlalchemy import DateTime as SQLDateTime
from sqlalchemy.dialects import mssql
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.settings import config
from src.sources.base import DataSource
from src.utils import retry

logger = logging.getLogger(__name__)


def sanitize_table_name(filename: str) -> str:
    name = Path(filename).stem
    # Replace invalid characters with underscore
    name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    # Ensure it starts with letter
    if not name[0].isalpha():
        name = f"t_{name}"
    return name


TYPE_MAPPING = {
    str: String,
    int: Integer,
    float: Numeric,
    bool: Boolean,
    Decimal: Numeric,
    DateTime: lambda: _get_timezone_aware_datetime_type(),
    Date: SQLDate,
    EmailStr: String,
}


def _get_timezone_aware_datetime_type():
    """Get the appropriate timezone-aware DateTime type for the current database dialect."""
    drivername = config.DRIVERNAME

    datetime_type_mapping = {
        "postgresql": SQLDateTime(timezone=True),  # TIMESTAMPTZ
        "mysql": SQLDateTime(
            timezone=False
        ),  # DATETIME doesn't support timezone, convert to UTC
        "mssql": mssql.DATETIMEOFFSET(2),  # DATETIMEOFFSET
        "sqlite": SQLDateTime(timezone=True),  # TEXT, timezone stored in value
    }

    for dialect_key, datetime_type in datetime_type_mapping.items():
        if dialect_key == drivername:
            return datetime_type

    logger.warning(
        f"Unknown database dialect '{drivername}', defaulting to DateTime(timezone=True)"
    )
    return SQLDateTime(timezone=True)


def _get_column_type(field_type):
    # Handle Optional types (Union[Type, None])
    if get_origin(field_type) is Union and type(None) in get_args(field_type):
        # Extract the non-None type from Optional[Type]
        inner_types = [t for t in get_args(field_type) if t is not type(None)]
        if len(inner_types) == 1:
            field_type = inner_types[0]

    if field_type in TYPE_MAPPING:
        mapped_type = TYPE_MAPPING[field_type]
        # Call lambda if it's callable, otherwise return the type directly
        if callable(mapped_type):
            return mapped_type()
        return mapped_type

    raise ValueError(f"Unsupported field type {field_type}")


def get_table_columns(source, include_timestamps: bool = True) -> list[Column]:
    columns = []

    for name, field in source.source_model.model_fields.items():
        field_type = field.annotation
        column_name = name

        is_nullable = (
            not field.is_required()
            or field.default is not None
            or field.default_factory is not None
        )

        sqlalchemy_type = _get_column_type(field_type)

        is_string_type = (
            sqlalchemy_type is String
            or isinstance(sqlalchemy_type, String)
            or (
                isinstance(sqlalchemy_type, type)
                and issubclass(sqlalchemy_type, String)
            )
        )

        if is_string_type:
            max_length = None
            if field.metadata:
                for meta in field.metadata:
                    if isinstance(meta, MaxLen):
                        max_length = meta.max_length
                        break

            # SQL Server requires explicit length - default to 255 if not specified
            if max_length is None and config.DRIVERNAME == "mssql":
                max_length = 255

            if max_length is not None:
                sqlalchemy_type = String(max_length)
            else:
                sqlalchemy_type = String()

        # For SQL Server, grain columns (primary keys) should not be identity columns
        autoincrement = None
        if config.DRIVERNAME == "mssql" and column_name in source.grain:
            autoincrement = False

        columns.append(
            Column(
                column_name,
                sqlalchemy_type,
                nullable=is_nullable,
                autoincrement=autoincrement,
            )
        )

    # SQLite requires Integer for auto-increment primary keys and foreign keys
    id_column_type = Integer if config.DRIVERNAME == "sqlite" else BigInteger

    columns.extend(
        [
            Column("etl_row_hash", LargeBinary(16), nullable=False),
            Column("source_filename", String(255), nullable=False),
            Column("file_load_log_id", id_column_type, nullable=False),
        ]
    )

    if include_timestamps:
        datetime_type = _get_timezone_aware_datetime_type()
        columns.append(Column("etl_created_at", datetime_type, nullable=False))
        columns.append(Column("etl_updated_at", datetime_type, nullable=True))

    return columns


def db_create_stage_table(
    engine: Engine, metadata: MetaData, source: DataSource, source_filename: str
) -> str:
    sanitized_name = sanitize_table_name(source_filename)
    stage_table_name = f"stage_{sanitized_name}"

    columns = get_table_columns(source, include_timestamps=False)

    stage_table = Table(stage_table_name, metadata, *columns)
    metadata.drop_all(engine, tables=[stage_table])
    metadata.create_all(engine, tables=[stage_table])
    return stage_table_name


@retry()
def db_check_if_duplicate_file(
    Session: sessionmaker[Session], source: DataSource, source_filename: str
) -> bool:
    with Session() as session:
        try:
            check_sql = text(
                f"SELECT CASE WHEN EXISTS(SELECT 1 FROM {source.table_name} WHERE source_filename = :filename) THEN 1 ELSE 0 END"
            )
            result = session.execute(check_sql, {"filename": source_filename}).scalar()
            return bool(result)
        except Exception as e:
            logger.exception(
                f"Error checking for duplicate file {source_filename} in {source.table_name}: {e}"
            )
            return False


def db_serialize_json_for_dlq_table(data: Dict[str, Any]) -> str:
    drivername = config.DRIVERNAME

    if drivername == "mssql":
        json_str = json.dumps(data, ensure_ascii=False)
        if len(json_str) > 4000:
            json_str = json_str[:3997] + "..."  # Leave room for "..."
            logger.warning(
                f"JSON data truncated to 4000 chars for SQL Server compatibility"
            )
        return json_str
    elif drivername == "sqlite":
        return json.dumps(data, ensure_ascii=False)
    else:
        return data


def db_create_row_hash(
    record: Dict[str, str], sorted_keys: tuple[str, ...] | None = None
) -> bytes:
    string_items = {
        key: str(value) if value is not None else "" for key, value in record.items()
    }

    data_string = "|".join(
        string_items[key] for key in sorted_keys if key in string_items
    )

    return xxhash.xxh32(data_string.encode("utf-8")).digest()


def db_get_column_names(source: DataSource) -> list[str]:
    columns = [column for column in source.source_model.model_fields.keys()]
    columns.extend(["etl_row_hash", "source_filename", "file_load_log_id"])
    return columns


@retry()
def db_start_log(
    Session: sessionmaker[Session],
    file_load_log_table: Table,
    source_filename: str,
    started_at: DateTime,
) -> int:
    stmt = insert(file_load_log_table).values(
        source_filename=source_filename, started_at=started_at
    )
    with Session() as session:
        try:
            res = session.execute(stmt)
            session.commit()
            return int(res.inserted_primary_key[0])
        except Exception as e:
            logger.exception(f"Error starting log for {source_filename}: {e}")
            session.rollback()
            raise e


def db_create_grain_validation_sql(source: DataSource) -> str:
    drivername = config.DRIVERNAME

    grain_sql = None

    if len(source.grain) == 1:
        grain_sql = f"SELECT CASE WHEN COUNT(DISTINCT {source.grain[0]}) = COUNT(*) THEN 1 ELSE 0 END AS grain_unique FROM {{table}}"
    else:
        if drivername == "mssql":
            # MSSQL requires concatenation for multiple columns
            concat_parts = []
            for col in source.grain:
                concat_parts.append(f"CAST({col} AS VARCHAR(4000))")
            concat_expr = " + '||' + ".join(concat_parts)
            grain_sql = f"SELECT CASE WHEN COUNT(DISTINCT {concat_expr}) = COUNT(*) THEN 1 ELSE 0 END AS grain_unique FROM {{table}}"
        elif drivername == "postgresql":
            # PostgreSQL uses tuple syntax for multiple columns
            grain_cols = ", ".join(source.grain)
            grain_sql = f"SELECT CASE WHEN COUNT(DISTINCT ({grain_cols})) = COUNT(*) THEN 1 ELSE 0 END AS grain_unique FROM {{table}}"
        elif drivername in ["mysql", "sqlite"]:
            # MySQL uses CONCAT for multiple columns
            concat_args = []
            for col in source.grain:
                concat_args.append(col)
                concat_args.append("'||'")
            # Remove the last '||' separator
            concat_args = concat_args[:-1]
            concat_expr = "CONCAT(" + ", ".join(concat_args) + ")"
            grain_sql = f"SELECT CASE WHEN COUNT(DISTINCT {concat_expr}) = COUNT(*) THEN 1 ELSE 0 END AS grain_unique FROM {{table}}"

        else:
            raise ValueError(f"Unsupported database dialect: {drivername}")

    return grain_sql


def db_create_duplicate_grain_examples_sql(source: DataSource, limit: int = 5) -> str:
    drivername = config.DRIVERNAME

    if drivername == "mssql":
        top_clause = f"SELECT TOP({limit})"
        bottom_clause = ""
    else:
        top_clause = "SELECT"
        bottom_clause = f"LIMIT {limit}"

    grain_cols = ", ".join(source.grain)
    duplicate_sql = f"""
    {top_clause}
    {grain_cols},
    COUNT(*) as duplicate_count
    FROM {{table}}
    GROUP BY {grain_cols}
    HAVING COUNT(*) > 1
    {bottom_clause}
    """
    return duplicate_sql
