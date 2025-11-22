from abc import ABC, abstractmethod
from pathlib import Path
from queue import Queue
from typing import Union


class BaseFileHelper(ABC):
    @classmethod
    @abstractmethod
    def scan_directory(cls, directory_path: Union[Path, str]) -> Queue:
        """
        Scan a directory (local or cloud) and return a queue of filenames.

        For local storage: directory_path is a Path object
        For cloud storage: directory_path is a URI string (e.g., 's3://bucket/path/')

        Returns a Queue[str] containing just the filenames (not full paths/URIs).
        """
        pass

    @classmethod
    @abstractmethod
    def copy_file_to_archive(cls, file_path: Union[Path, str]):
        """
        Copy a file to the archive location.

        For local storage: file_path is a Path object
        For cloud storage: file_path is a URI string (e.g., 's3://bucket/path/file.csv')
        """
        pass

    @classmethod
    @abstractmethod
    def copy_file_to_duplicate_files(cls, file_path: Union[Path, str]):
        """
        Move a file to the duplicate files location.

        For local storage: file_path is a Path object
        For cloud storage: file_path is a URI string
        """
        pass

    @classmethod
    @abstractmethod
    def delete_file(cls, file_path: Union[Path, str]) -> None:
        """
        Delete a file.

        For local storage: file_path is a Path object
        For cloud storage: file_path is a URI string
        """
        pass

    @classmethod
    @abstractmethod
    def get_file_path(
        cls, directory_path: Union[Path, str], filename: str
    ) -> Union[Path, str]:
        """
        Construct the full file path/URI from directory and filename.

        For local storage: returns Path(directory_path / filename)
        For cloud storage: returns URI string (e.g., 's3://bucket/path/filename.csv')
        """
        pass

    @classmethod
    @abstractmethod
    def download_to_local(cls, file_path: Union[Path, str]) -> Path:
        """
        Ensure a file is available locally, downloading from cloud if necessary.

        For local storage: returns the Path as-is
        For cloud storage: downloads the file to a temporary location and returns that Path.
        The caller is responsible for cleanup of temporary files.

        Returns a local Path that can be used with standard file I/O operations.
        """
        pass
