import os

# Needs to happen before local imports
os.environ["ENV_STATE"] = "test"
from pathlib import Path

import pytest

from src.process.db import create_tables, setup_db
from src.settings import config
from src.sources.master import MASTER_REGISTRY
from src.tests.fixtures.sources import (
    TEST_CSV_SOURCE,
    TEST_EXCEL_SOURCE,
    TEST_JSON_SOURCE,
)


@pytest.fixture
def temp_directory(tmp_path):
    return Path(tmp_path)


@pytest.fixture
def temp_sqlite_db(tmp_path):
    db_path = tmp_path / "test.db"
    database_url = f"sqlite:///{db_path}"
    config.DATABASE_URL = database_url
    engine, metadata = setup_db()
    sources = [
        TEST_CSV_SOURCE,
        TEST_EXCEL_SOURCE,
        TEST_JSON_SOURCE,
    ]
    MASTER_REGISTRY.add_sources(sources)
    create_tables(metadata, engine)

    yield engine

    metadata.drop_all(engine)
    engine.dispose()
