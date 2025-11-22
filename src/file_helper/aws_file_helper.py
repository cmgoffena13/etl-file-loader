import logging
from contextlib import contextmanager
from pathlib import Path
from queue import Queue
from typing import Union
from urllib.parse import urlparse

import boto3
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


class AWSFileHelper(BaseFileHelper):
    @classmethod
    def _parse_s3_uri(cls, uri: str) -> tuple[str, str]:
        """Parse S3 URI (s3://bucket/key) into bucket and key."""
        parsed = urlparse(uri)
        if parsed.scheme != "s3":
            raise ValueError(f"Invalid S3 URI: {uri}")
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        return bucket, key

    @classmethod
    def _get_s3_client(cls):
        """Get S3 client. Uses default credentials from environment or IAM role."""
        return boto3.client("s3")

    @classmethod
    def scan_directory(cls, directory_path: Union[Path, str]) -> Queue:
        """Scan S3 bucket/prefix and return queue of filenames."""
        if isinstance(directory_path, Path):
            raise ValueError("AWSFileHelper requires S3 URI, not local Path")

        bucket, prefix = cls._parse_s3_uri(str(directory_path))
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        s3_client = cls._get_s3_client()
        file_paths_queue = Queue()

        try:
            paginator = s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                if "Contents" in page:
                    for obj in page["Contents"]:
                        key = obj["Key"]
                        # Get just the filename (last part of key)
                        filename = key.split("/")[-1]
                        if filename and not filename.startswith("."):
                            file_paths_queue.put(filename)
        except s3_client.exceptions.NoSuchBucket:
            raise DirectoryNotFoundError(f"S3 bucket not found: {bucket}")

        return file_paths_queue

    @classmethod
    def copy_file_to_archive(cls, file_path: Union[Path, str]):
        """Copy S3 object to archive location."""
        if isinstance(file_path, Path):
            raise ValueError("AWSFileHelper requires S3 URI, not local Path")

        bucket, source_key = cls._parse_s3_uri(str(file_path))
        filename = source_key.split("/")[-1]

        # Parse archive path (should be S3 URI)
        archive_uri = str(config.ARCHIVE_PATH)
        archive_bucket, archive_prefix = cls._parse_s3_uri(archive_uri)
        archive_key = (
            f"{archive_prefix.rstrip('/')}/{filename}" if archive_prefix else filename
        )

        s3_client = cls._get_s3_client()
        try:
            copy_source = {"Bucket": bucket, "Key": source_key}
            s3_client.copy_object(
                CopySource=copy_source,
                Bucket=archive_bucket,
                Key=archive_key,
            )
        except Exception as e:
            raise FileCopyError(
                f"Failed to copy S3 object from {file_path} to {archive_uri}/{filename}: {e}"
            )

    @classmethod
    def copy_file_to_duplicate_files(cls, file_path: Union[Path, str]):
        """Move S3 object to duplicate files location."""
        if isinstance(file_path, Path):
            raise ValueError("AWSFileHelper requires S3 URI, not local Path")

        bucket, source_key = cls._parse_s3_uri(str(file_path))
        filename = source_key.split("/")[-1]
        stem = Path(filename).stem
        suffix = Path(filename).suffix

        # Parse duplicate files path (should be S3 URI)
        duplicate_uri = str(config.DUPLICATE_FILES_PATH)
        duplicate_bucket, duplicate_prefix = cls._parse_s3_uri(duplicate_uri)

        # Check if destination exists and add timestamp if needed
        destination_key = (
            f"{duplicate_prefix.rstrip('/')}/{filename}"
            if duplicate_prefix
            else filename
        )
        s3_client = cls._get_s3_client()
        try:
            s3_client.head_object(Bucket=duplicate_bucket, Key=destination_key)
            # File exists, add timestamp
            timestamp = pendulum.now("UTC").format("YYYYMMDD_HHmmss")
            destination_key = (
                f"{duplicate_prefix.rstrip('/')}/{stem}_{timestamp}{suffix}"
                if duplicate_prefix
                else f"{stem}_{timestamp}{suffix}"
            )
        except s3_client.exceptions.ClientError:
            # File doesn't exist, use original name
            pass

        try:
            # Copy then delete (S3 doesn't have move)
            copy_source = {"Bucket": bucket, "Key": source_key}
            s3_client.copy_object(
                CopySource=copy_source,
                Bucket=duplicate_bucket,
                Key=destination_key,
            )
            s3_client.delete_object(Bucket=bucket, Key=source_key)
        except Exception as e:
            raise FileMoveError(
                f"Failed to move S3 object from {file_path} to {duplicate_uri}/{destination_key.split('/')[-1]}: {e}"
            )

    @classmethod
    def delete_file(cls, file_path: Union[Path, str]) -> None:
        """Delete S3 object."""
        if isinstance(file_path, Path):
            raise ValueError("AWSFileHelper requires S3 URI, not local Path")

        bucket, key = cls._parse_s3_uri(str(file_path))
        s3_client = cls._get_s3_client()
        try:
            s3_client.delete_object(Bucket=bucket, Key=key)
        except Exception as e:
            raise FileDeleteError(f"Failed to delete S3 object {file_path}: {e}")

    @classmethod
    def get_file_path(
        cls, directory_path: Union[Path, str], filename: str
    ) -> Union[Path, str]:
        """Construct S3 URI from directory and filename."""
        if isinstance(directory_path, Path):
            raise ValueError("AWSFileHelper requires S3 URI, not local Path")

        directory_uri = str(directory_path).rstrip("/")
        return f"{directory_uri}/{filename}"

    @classmethod
    @contextmanager
    def get_file_stream(cls, file_path: Union[Path, str], mode: str = "rb"):
        """Get streaming download from S3."""
        if isinstance(file_path, Path):
            raise ValueError("AWSFileHelper requires S3 URI, not local Path")

        if mode != "rb":
            raise ValueError("S3 streams are always binary, mode must be 'rb'")

        bucket, key = cls._parse_s3_uri(str(file_path))
        s3_client = cls._get_s3_client()

        try:
            response = s3_client.get_object(Bucket=bucket, Key=key)
            yield response["Body"]
        except s3_client.exceptions.NoSuchKey:
            raise FileNotFoundError(f"S3 object not found: {file_path}")
        except Exception as e:
            raise IOError(f"Failed to stream S3 object {file_path}: {e}")
