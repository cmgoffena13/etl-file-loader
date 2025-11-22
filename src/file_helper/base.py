from abc import ABC, abstractmethod
from pathlib import Path
from queue import Queue


class BaseFileHelper(ABC):
    @classmethod
    @abstractmethod
    def scan_directory(cls, directory_path: Path) -> Queue:
        pass

    @classmethod
    @abstractmethod
    def copy_file_to_archive(cls, file_path: Path):
        pass

    @classmethod
    @abstractmethod
    def copy_file_to_duplicate_files(cls, file_path: Path):
        pass

    @classmethod
    @abstractmethod
    def delete_file(cls, file_path: Path) -> None:
        pass
