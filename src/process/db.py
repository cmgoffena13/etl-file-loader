import logging
from sqlite3 import register_adapter

import pendulum
import pymysql.converters
from pydantic_extra_types.pendulum_dt import Date, DateTime
from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    create_engine,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Engine

from src.pipeline.db_utils import (
    _get_timezone_aware_datetime_type,
    get_table_columns,
)
from src.settings import DevConfig, config, get_database_config
from src.sources.master import MASTER_REGISTRY

logger = logging.getLogger(__name__)


def _register_pendulum_adapters():
    drivername = config.DRIVERNAME
    if drivername == "sqlite":
        register_adapter(pendulum.DateTime, lambda val: val.isoformat(" "))
        register_adapter(pendulum.Date, lambda val: val.format("YYYY-MM-DD"))
        register_adapter(DateTime, lambda val: val.in_timezone("UTC").isoformat(" "))
        register_adapter(Date, lambda val: val.format("YYYY-MM-DD"))
    elif drivername == "mysql":
        pymysql.converters.conversions[pendulum.DateTime] = (
            pymysql.converters.escape_datetime
        )
        pymysql.converters.conversions[DateTime] = pymysql.converters.escape_datetime


def setup_db():
    _register_pendulum_adapters()
    db_config = get_database_config()

    engine_kwargs = {
        "url": db_config["sqlalchemy.url"],
        "echo": db_config["sqlalchemy.echo"],
        "future": db_config["sqlalchemy.future"],
        "connect_args": db_config.get("sqlalchemy.connect_args", {}),
        "pool_size": db_config.get("sqlalchemy.pool_size", 20),
    }
    if "sqlalchemy.max_overflow" in db_config:
        engine_kwargs["max_overflow"] = db_config["sqlalchemy.max_overflow"]
    if "sqlalchemy.pool_timeout" in db_config:
        engine_kwargs["pool_timeout"] = db_config["sqlalchemy.pool_timeout"]

    engine = create_engine(**engine_kwargs)
    metadata = MetaData()

    return engine, metadata


def _get_json_column_type():
    drivername = config.DRIVERNAME

    json_column_mapping = {
        "postgresql": JSONB,
        "mysql": JSON,
        "mssql": String(4000),
        "sqlite": Text,
        "bigquery": String,  # BigQuery stores JSON as STRING
    }

    for dialect_key, column_type in json_column_mapping.items():
        if dialect_key == drivername:
            return column_type

    logger.warning(f"Unknown database dialect '{drivername}', defaulting to JSON")
    return JSON


def create_tables(metadata: MetaData, engine: Engine):
    tables = []
    for source in MASTER_REGISTRY.sources:
        columns = get_table_columns(source, include_timestamps=True)
        if len(source.grain) > 3:
            logger.warning(
                f"Source {source.table_name} has more than 3 grain columns. Inefficient primary key."
            )
        primary_key = PrimaryKeyConstraint(*source.grain)

        source_table_kwargs = {}
        if config.DRIVERNAME == "bigquery":
            source_table_kwargs["bigquery_clustering_fields"] = source.grain
        table = Table(
            source.table_name,
            metadata,
            *columns,
            primary_key,
            **source_table_kwargs,
        )
        # define index separately, bound to table column
        if config.DRIVERNAME != "bigquery":
            Index(f"idx_{source.table_name}_source_filename", table.c.source_filename)
        tables.append(table)

    # SQLite requires Integer for auto-increment primary keys
    id_column_type = Integer if config.DRIVERNAME == "sqlite" else BigInteger
    id_autoincrement = False if config.DRIVERNAME == "bigquery" else True
    datetime_type = _get_timezone_aware_datetime_type()

    table_kwargs = {}
    if config.DRIVERNAME == "bigquery":
        table_kwargs["bigquery_clustering_fields"] = ["source_filename", "id"]

    file_load_log = Table(
        "file_load_log",
        metadata,
        Column("id", id_column_type, primary_key=True, autoincrement=id_autoincrement),
        Column("source_filename", String(255), nullable=False),
        Column("started_at", datetime_type, nullable=False),
        Column("duplicate_skipped", Boolean, nullable=True),
        # archive copy phase
        Column("archive_copy_started_at", datetime_type, nullable=True),
        Column("archive_copy_ended_at", datetime_type, nullable=True),
        Column("archive_copy_success", Boolean, nullable=True),
        # reading phase
        Column("read_started_at", datetime_type, nullable=True),
        Column("read_ended_at", datetime_type, nullable=True),
        Column("read_success", Boolean, nullable=True),
        Column("records_read", Integer, nullable=True),
        # validating phase
        Column("validate_started_at", datetime_type, nullable=True),
        Column("validate_ended_at", datetime_type, nullable=True),
        Column("validate_success", Boolean, nullable=True),
        Column("validation_errors", Integer, nullable=True),
        # stage load phase
        Column("write_started_at", datetime_type, nullable=True),
        Column("write_ended_at", datetime_type, nullable=True),
        Column("write_success", Boolean, nullable=True),
        Column("records_written_to_stage", Integer, nullable=True),
        # audit phase
        Column("audit_started_at", datetime_type, nullable=True),
        Column("audit_ended_at", datetime_type, nullable=True),
        Column("audit_success", Boolean, nullable=True),
        # merge phase
        Column("publish_started_at", datetime_type, nullable=True),
        Column("publish_ended_at", datetime_type, nullable=True),
        Column("publish_success", Boolean, nullable=True),
        Column("publish_inserts", Integer, nullable=True),
        Column("publish_updates", Integer, nullable=True),
        # summary
        Column("ended_at", datetime_type, nullable=True),
        Column("success", Boolean, nullable=True),
        Column("error_type", String(50), nullable=True),
        **table_kwargs,
    )
    if config.DRIVERNAME != "bigquery":
        Index(
            "idx_file_load_log_source_filename",
            file_load_log.c.source_filename,
            file_load_log.c.id,
        )
    tables.append(file_load_log)

    # Dead Letter Queue table for validation failures
    # Use appropriate JSON column type based on database backend
    json_column_type = _get_json_column_type()

    table_kwargs = {}
    if config.DRIVERNAME == "bigquery":
        table_kwargs["bigquery_clustering_fields"] = ["source_filename", "id"]

    file_load_dlq = Table(
        "file_load_dlq",
        metadata,
        Column("id", id_column_type, primary_key=True, autoincrement=id_autoincrement),
        Column("source_filename", String(255), nullable=False),
        Column("file_row_number", Integer, nullable=False),
        Column("file_record_data", json_column_type, nullable=False),
        Column("validation_errors", json_column_type, nullable=False),
        Column(
            "file_load_log_id",
            id_column_type,
            ForeignKey("file_load_log.id"),
            nullable=False,
        ),
        Column("target_table_name", String(255), nullable=False),
        Column("failed_at", datetime_type, nullable=False),
        **table_kwargs,
    )
    if config.DRIVERNAME != "bigquery":
        Index("idx_dlq_file_load_log_id", file_load_dlq.c.file_load_log_id)
        Index(
            "idx_dlq_source_filename",
            file_load_dlq.c.source_filename,
            file_load_dlq.c.id,
        )
    tables.append(file_load_dlq)
    if isinstance(config, DevConfig):
        metadata.drop_all(engine, tables=tables)
    metadata.create_all(engine, tables=tables)
