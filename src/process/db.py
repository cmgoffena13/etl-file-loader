from sqlite3 import register_adapter

import pendulum
import pymysql.converters
from pydantic_extra_types.pendulum_dt import Date, DateTime
from sqlalchemy import MetaData, create_engine

from src.settings import config, get_database_config


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
