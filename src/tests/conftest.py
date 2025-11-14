import os

# Needs to happen before local imports
os.environ["ENV_STATE"] = "test"

import csv
import json
from pathlib import Path

import pytest


@pytest.fixture
def temp_directory(tmp_path):
    return Path(tmp_path)
