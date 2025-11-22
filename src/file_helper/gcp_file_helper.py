import logging
import os
from contextlib import contextmanager
from pathlib import Path
from queue import Queue
from typing import Union
from urllib.parse import urlparse

import pendulum
from google.cloud import storage
from google.cloud.exceptions import NotFound

from src.exception.exceptions import (
    DirectoryNotFoundError,
    FileCopyError,
    FileDeleteError,
    FileMoveError,
)
from src.file_helper.base import BaseFileHelper
from src.settings import config
from src.utils import retry

if config.GOOGLE_APPLICATION_CREDENTIALS:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config.GOOGLE_APPLICATION_CREDENTIALS

logger = logging.getLogger(__name__)


class GCPFileHelper(BaseFileHelper):
    _storage_client = None

    @classmethod
    def _parse_gcs_uri(cls, uri: str) -> tuple[str, str]:
        """Parse GCS URI (gs://bucket/blob-path) into bucket and blob name."""
        parsed = urlparse(uri)
        if parsed.scheme != "gs":
            raise ValueError(f"Invalid GCS URI: {uri}")
        bucket = parsed.netloc
        blob_name = parsed.path.lstrip("/")
        return bucket, blob_name

    @classmethod
    def _get_storage_client(cls):
        if cls._storage_client is None:
            client_kwargs = {}
            if config.GCP_PROJECT_ID:
                client_kwargs["project"] = config.GCP_PROJECT_ID
            cls._storage_client = storage.Client(**client_kwargs)

        return cls._storage_client

    @classmethod
    @retry()
    def scan_directory(cls, directory_path: Union[Path, str]) -> Queue:
        """Scan GCS bucket/prefix and return queue of filenames."""
        if isinstance(directory_path, Path):
            raise ValueError("GCPFileHelper requires GCS URI, not local Path")

        logger.info(f"Scanning GCS directory: {directory_path}")

        bucket_name, prefix = cls._parse_gcs_uri(str(directory_path))
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        storage_client = cls._get_storage_client()
        file_paths_queue = Queue()

        try:
            bucket = storage_client.bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=prefix)
            for blob in blobs:
                blob_name = blob.name
                # Get just the filename (last part of blob name)
                filename = blob_name.split("/")[-1]
                if filename and not filename.startswith("."):
                    file_paths_queue.put(filename)
        except Exception as e:
            raise DirectoryNotFoundError(
                f"Failed to list blobs in GCS bucket {bucket_name}: {e}"
            )

        return file_paths_queue

    @classmethod
    @retry()
    def copy_file_to_archive(cls, file_path: Union[Path, str]):
        """Copy GCS blob to archive location."""
        if isinstance(file_path, Path):
            raise ValueError("GCPFileHelper requires GCS URI, not local Path")

        source_bucket_name, source_blob_name = cls._parse_gcs_uri(str(file_path))
        filename = source_blob_name.split("/")[-1]

        # Parse archive path (should be GCS URI)
        archive_uri = str(config.ARCHIVE_PATH)
        archive_bucket_name, archive_prefix = cls._parse_gcs_uri(archive_uri)
        archive_blob_name = (
            f"{archive_prefix.rstrip('/')}/{filename}" if archive_prefix else filename
        )
        archive_path = f"gs://{archive_bucket_name}/{archive_blob_name}"

        storage_client = cls._get_storage_client()
        logger.info(f"Copying GCS blob from {file_path} to {archive_path}")
        try:
            source_bucket = storage_client.bucket(source_bucket_name)
            source_blob = source_bucket.blob(source_blob_name)
            dest_bucket = storage_client.bucket(archive_bucket_name)
            source_bucket.copy_blob(source_blob, dest_bucket, archive_blob_name)
        except Exception as e:
            raise FileCopyError(
                f"Failed to copy GCS blob from {file_path} to {archive_uri}/{filename}: {e}"
            )

    @classmethod
    @retry()
    def copy_file_to_duplicate_files(cls, file_path: Union[Path, str]):
        """Move GCS blob to duplicate files location."""
        if isinstance(file_path, Path):
            raise ValueError("GCPFileHelper requires GCS URI, not local Path")

        source_bucket_name, source_blob_name = cls._parse_gcs_uri(str(file_path))
        filename = source_blob_name.split("/")[-1]
        stem = Path(filename).stem
        suffix = Path(filename).suffix

        # Parse duplicate files path (should be GCS URI)
        duplicate_uri = str(config.DUPLICATE_FILES_PATH)
        duplicate_bucket_name, duplicate_prefix = cls._parse_gcs_uri(duplicate_uri)

        # Check if destination exists and add timestamp if needed
        destination_blob_name = (
            f"{duplicate_prefix.rstrip('/')}/{filename}"
            if duplicate_prefix
            else filename
        )
        storage_client = cls._get_storage_client()
        dest_bucket = storage_client.bucket(duplicate_bucket_name)
        try:
            dest_bucket.blob(destination_blob_name).reload()
            # File exists, add timestamp
            timestamp = pendulum.now("UTC").format("YYYYMMDD_HHmmss")
            destination_blob_name = (
                f"{duplicate_prefix.rstrip('/')}/{stem}_{timestamp}{suffix}"
                if duplicate_prefix
                else f"{stem}_{timestamp}{suffix}"
            )
        except NotFound:
            # File doesn't exist, use original name
            pass

        destination_path = f"gs://{duplicate_bucket_name}/{destination_blob_name}"

        try:
            logger.info(f"Moving GCS blob from {file_path} to {destination_path}")
            source_bucket = storage_client.bucket(source_bucket_name)
            source_blob = source_bucket.blob(source_blob_name)
            dest_bucket = storage_client.bucket(duplicate_bucket_name)
            # Copy then delete (GCS doesn't have move)
            source_bucket.copy_blob(source_blob, dest_bucket, destination_blob_name)
            source_blob.delete()
        except Exception as e:
            raise FileMoveError(
                f"Failed to move GCS blob from {file_path} to {duplicate_uri}/{destination_blob_name.split('/')[-1]}: {e}"
            )

    @classmethod
    @retry()
    def delete_file(cls, file_path: Union[Path, str]) -> None:
        """Delete GCS blob."""
        if isinstance(file_path, Path):
            raise ValueError("GCPFileHelper requires GCS URI, not local Path")

        bucket_name, blob_name = cls._parse_gcs_uri(str(file_path))
        storage_client = cls._get_storage_client()
        logger.info(f"Deleting GCS blob: {file_path}")
        try:
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            blob.delete()
        except Exception as e:
            raise FileDeleteError(f"Failed to delete GCS blob {file_path}: {e}")

    @classmethod
    def get_file_path(
        cls, directory_path: Union[Path, str], filename: str
    ) -> Union[Path, str]:
        """Construct GCS URI from directory and filename."""
        if isinstance(directory_path, Path):
            raise ValueError("GCPFileHelper requires GCS URI, not local Path")

        directory_uri = str(directory_path).rstrip("/")
        return f"{directory_uri}/{filename}"

    @classmethod
    @contextmanager
    @retry()
    def get_file_stream(cls, file_path: Union[Path, str], mode: str = "rb"):
        """Get streaming download from GCS."""
        if isinstance(file_path, Path):
            raise ValueError("GCPFileHelper requires GCS URI, not local Path")

        if mode != "rb":
            raise ValueError("GCS streams are always binary, mode must be 'rb'")

        bucket_name, blob_name = cls._parse_gcs_uri(str(file_path))
        storage_client = cls._get_storage_client()

        try:
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            yield blob.open("rb")
        except Exception as e:
            if "not found" in str(e).lower() or "404" in str(e):
                raise FileNotFoundError(f"GCS blob not found: {file_path}")
            raise IOError(f"Failed to stream GCS blob {file_path}: {e}")
