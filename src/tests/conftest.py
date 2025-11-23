import os

# Needs to happen before local imports
os.environ["ENV_STATE"] = "test"
import csv
import gzip
import json
from typing import Any
from unittest.mock import MagicMock, patch

import pyexcel
import pytest

from src.process.processor import Processor
from src.settings import config
from src.sources.master import MASTER_REGISTRY
from src.tests.fixtures.sources import (
    TEST_CSV_GZ_SOURCE,
    TEST_CSV_SOURCE,
    TEST_CSV_SOURCE_WITH_NOTIFICATIONS,
    TEST_CSV_SOURCE_WITH_THRESHOLD,
    TEST_EXCEL_SOURCE,
    TEST_JSON_GZ_SOURCE,
    TEST_JSON_SOURCE,
)


@pytest.fixture()
def session_temp_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("test_session")


@pytest.fixture(autouse=True)
def setup_test_db_and_directories(session_temp_dir):
    config.DIRECTORY_PATH = session_temp_dir
    config.ARCHIVE_PATH = session_temp_dir / "archive"
    config.DUPLICATE_FILES_PATH = session_temp_dir / "duplicate_files"
    config.ARCHIVE_PATH.mkdir(exist_ok=True)
    config.DUPLICATE_FILES_PATH.mkdir(exist_ok=True)

    MASTER_REGISTRY.sources.clear()

    test_sources = [
        TEST_CSV_SOURCE,
        TEST_CSV_GZ_SOURCE,
        TEST_CSV_SOURCE_WITH_NOTIFICATIONS,
        TEST_CSV_SOURCE_WITH_THRESHOLD,
        TEST_EXCEL_SOURCE,
        TEST_JSON_SOURCE,
        TEST_JSON_GZ_SOURCE,
    ]
    MASTER_REGISTRY.add_sources(test_sources)


@pytest.fixture(autouse=True)
def test_processor(setup_test_db_and_directories):
    processor = Processor()
    yield processor


@pytest.fixture()
def create_csv_file(session_temp_dir):
    file_paths = []

    def _create_csv_file(file_name: str, data: list[list[str]]):
        file_path = session_temp_dir / file_name
        with open(file_path, "w") as f:
            writer = csv.writer(f)
            writer.writerows(data)
        file_paths.append(file_path)
        return file_name

    yield _create_csv_file

    for file_path in file_paths:
        if file_path.exists():
            file_path.unlink()


@pytest.fixture()
def create_excel_file(session_temp_dir):
    file_paths = []

    def _create_excel_file(file_name: str, data: list[list]):
        file_path = session_temp_dir / file_name
        pyexcel.save_as(
            array=data, dest_file_name=str(file_path), name_columns_by_row=0
        )
        file_paths.append(file_path)
        return file_name

    yield _create_excel_file

    for file_path in file_paths:
        if file_path.exists():
            file_path.unlink()


@pytest.fixture()
def create_csv_gz_file(session_temp_dir):
    file_paths = []

    def _create_csv_gz_file(file_name: str, data: list[list[str]]):
        file_path = session_temp_dir / file_name
        with gzip.open(file_path, "wt", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(data)
        file_paths.append(file_path)
        return file_name

    yield _create_csv_gz_file

    for file_path in file_paths:
        if file_path.exists():
            file_path.unlink()


@pytest.fixture()
def create_json_file(session_temp_dir):
    file_paths = []

    def _create_json_file(file_name: str, data: dict[str, Any]):
        file_path = session_temp_dir / file_name
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        file_paths.append(file_path)
        return file_name

    yield _create_json_file

    for file_path in file_paths:
        if file_path.exists():
            file_path.unlink()


@pytest.fixture()
def create_json_gz_file(session_temp_dir):
    file_paths = []

    def _create_json_gz_file(file_name: str, data: dict[str, Any]):
        file_path = session_temp_dir / file_name
        with gzip.open(file_path, "wt", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        file_paths.append(file_path)
        return file_name

    yield _create_json_gz_file

    for file_path in file_paths:
        if file_path.exists():
            file_path.unlink()


@pytest.fixture()
def mock_email_notify():
    """Mock email notification function."""
    with patch("src.notify.email.EmailNotifier.notify") as mock:
        yield mock


@pytest.fixture()
def mock_slack_notify():
    """Mock Slack notification function."""
    with patch("src.notify.slack.SlackNotifier.notify") as mock:
        yield mock
