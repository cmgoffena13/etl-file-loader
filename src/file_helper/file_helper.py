import logging
import os
import shutil
from pathlib import Path
from queue import Queue

import pendulum

from src.exception.exceptions import (
    DirectoryNotFoundError,
    FileCopyError,
    FileDeleteError,
    FileMoveError,
)
from src.file_helper.base import BaseFileHelper
from src.settings import config

logger = logging.getLogger(__name__)


class FileHelper(BaseFileHelper):
    @classmethod
    def scan_directory(cls, directory_path: Path) -> Queue:
        if not directory_path.exists():
            raise DirectoryNotFoundError(f"Directory not found: {directory_path}")

        file_paths_queue = Queue()
        for entry in os.scandir(directory_path):
            if entry.is_file() and not entry.name.startswith("."):
                file_paths_queue.put(entry.name)

        return file_paths_queue

    @classmethod
    def copy_file_to_archive(cls, file_path: Path):
        try:
            shutil.copyfile(file_path, config.ARCHIVE_PATH / file_path.name)
        except Exception as e:
            raise FileCopyError(
                f"Failed to copy file from {file_path} to {config.ARCHIVE_PATH / file_path.name}: {e}"
            )

    @classmethod
    def copy_file_to_duplicate_files(cls, file_path: Path):
        destination = config.DUPLICATE_FILES_PATH / file_path.name
        if destination.exists():
            timestamp = pendulum.now("UTC").format("YYYYMMDD_HHmmss")
            stem = file_path.stem
            suffix = file_path.suffix
            destination = config.DUPLICATE_FILES_PATH / f"{stem}_{timestamp}{suffix}"
        try:
            shutil.move(file_path, destination)
        except Exception as e:
            raise FileMoveError(
                f"Failed to move file from {file_path} to {destination}: {e}"
            )

    @classmethod
    def delete_file(cls, file_path: Path) -> None:
        try:
            file_path.unlink()
        except FileNotFoundError:
            pass
        except Exception as e:
            raise FileDeleteError(f"Failed to delete file {file_path}: {e}")

    @classmethod
    def get_file_path(cls, directory_path: Path, filename: str) -> Path:
        return Path(directory_path / filename)
