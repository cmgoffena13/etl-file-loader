import logging
from contextlib import contextmanager
from pathlib import Path
from queue import Queue
from typing import Union
from urllib.parse import urlparse

import pendulum
from azure.core.credentials import AzureNamedKeyCredential
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

from src.exception.exceptions import (
    DirectoryNotFoundError,
    FileCopyError,
    FileDeleteError,
    FileMoveError,
)
from src.file_helper.base import BaseFileHelper
from src.settings import config
from src.utils import retry

logger = logging.getLogger(__name__)


class AzureFileHelper(BaseFileHelper):
    _blob_service_client = None

    @classmethod
    def _parse_azure_uri(cls, uri: str) -> tuple[str, str, str]:
        """Parse Azure Blob URI into account, container, and blob name."""
        parsed = urlparse(uri)
        if parsed.scheme == "azure":
            # Format: azure://container/blob-path
            container = parsed.netloc
            blob_name = parsed.path.lstrip("/")
            # Account name from environment or config
            account_name = config.AZURE_STORAGE_ACCOUNT_NAME
            if not account_name:
                raise ValueError("AZURE_STORAGE_ACCOUNT_NAME must be set in config")
            return account_name, container, blob_name
        elif parsed.scheme == "https":
            # Format: https://account.blob.core.windows.net/container/blob-path
            account_name = parsed.netloc.split(".")[0]
            path_parts = parsed.path.lstrip("/").split("/", 1)
            container = path_parts[0]
            blob_name = path_parts[1] if len(path_parts) > 1 else ""
            return account_name, container, blob_name
        else:
            raise ValueError(f"Invalid Azure Blob URI: {uri}")

    @classmethod
    def _get_blob_service_client(cls, client=None):
        if client is not None:
            cls._blob_service_client = client
            return client

        if cls._blob_service_client is None:
            connection_string = getattr(config, "AZURE_STORAGE_CONNECTION_STRING", None)
            if connection_string:
                cls._blob_service_client = BlobServiceClient.from_connection_string(
                    connection_string
                )
                return cls._blob_service_client

            account_name = getattr(config, "AZURE_STORAGE_ACCOUNT_NAME", None)
            account_key = getattr(config, "AZURE_STORAGE_ACCOUNT_KEY", None)
            if account_name and account_key:
                credential = AzureNamedKeyCredential(account_name, account_key)
                cls._blob_service_client = BlobServiceClient(
                    account_url=f"https://{account_name}.blob.core.windows.net",
                    credential=credential,
                )
                return cls._blob_service_client

            # Try default credential chain (Managed Identity, etc.)
            account_name = getattr(config, "AZURE_STORAGE_ACCOUNT_NAME", None)
            if account_name:
                cls._blob_service_client = BlobServiceClient(
                    account_url=f"https://{account_name}.blob.core.windows.net",
                    credential=DefaultAzureCredential(),
                )
                return cls._blob_service_client

            raise ValueError(
                "Azure credentials not configured. Set AZURE_STORAGE_CONNECTION_STRING, "
                "or AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY"
            )

        return cls._blob_service_client

    @classmethod
    @retry()
    def scan_directory(cls, directory_path: Union[Path, str]) -> Queue:
        """Scan Azure Blob container/prefix and return queue of filenames."""
        if isinstance(directory_path, Path):
            raise ValueError("AzureFileHelper requires Azure Blob URI, not local Path")

        _, container, prefix = cls._parse_azure_uri(str(directory_path))
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        blob_service_client = cls._get_blob_service_client()
        container_client = blob_service_client.get_container_client(container)

        file_paths_queue = Queue()

        try:
            blobs = container_client.list_blobs(name_starts_with=prefix)
            for blob in blobs:
                blob_name = blob.name
                # Get just the filename (last part of blob name)
                filename = blob_name.split("/")[-1]
                if filename and not filename.startswith("."):
                    file_paths_queue.put(filename)
        except Exception as e:
            raise DirectoryNotFoundError(
                f"Failed to list blobs in container {container}: {e}"
            )

        return file_paths_queue

    @classmethod
    @retry()
    def copy_file_to_archive(cls, file_path: Union[Path, str]):
        """Copy Azure Blob to archive location."""
        if isinstance(file_path, Path):
            raise ValueError("AzureFileHelper requires Azure Blob URI, not local Path")

        _, source_container, source_blob = cls._parse_azure_uri(str(file_path))
        filename = source_blob.split("/")[-1]

        # Parse archive path (should be Azure Blob URI)
        archive_uri = str(config.ARCHIVE_PATH)
        _, archive_container, archive_prefix = cls._parse_azure_uri(archive_uri)
        archive_blob = (
            f"{archive_prefix.rstrip('/')}/{filename}" if archive_prefix else filename
        )

        blob_service_client = cls._get_blob_service_client()
        try:
            source_blob_client = blob_service_client.get_blob_client(
                container=source_container, blob=source_blob
            )
            dest_blob_client = blob_service_client.get_blob_client(
                container=archive_container, blob=archive_blob
            )
            dest_blob_client.start_copy_from_url(source_blob_client.url)
        except Exception as e:
            raise FileCopyError(
                f"Failed to copy Azure Blob from {file_path} to {archive_uri}/{filename}: {e}"
            )

    @classmethod
    @retry()
    def copy_file_to_duplicate_files(cls, file_path: Union[Path, str]):
        """Move Azure Blob to duplicate files location."""
        if isinstance(file_path, Path):
            raise ValueError("AzureFileHelper requires Azure Blob URI, not local Path")

        _, source_container, source_blob = cls._parse_azure_uri(str(file_path))
        filename = source_blob.split("/")[-1]
        stem = Path(filename).stem
        suffix = Path(filename).suffix

        # Parse duplicate files path (should be Azure Blob URI)
        duplicate_uri = str(config.DUPLICATE_FILES_PATH)
        _, duplicate_container, duplicate_prefix = cls._parse_azure_uri(duplicate_uri)

        # Check if destination exists and add timestamp if needed
        destination_blob = (
            f"{duplicate_prefix.rstrip('/')}/{filename}"
            if duplicate_prefix
            else filename
        )
        blob_service_client = cls._get_blob_service_client()
        dest_container_client = blob_service_client.get_container_client(
            duplicate_container
        )
        try:
            dest_container_client.get_blob_client(
                destination_blob
            ).get_blob_properties()
            # File exists, add timestamp
            timestamp = pendulum.now("UTC").format("YYYYMMDD_HHmmss")
            destination_blob = (
                f"{duplicate_prefix.rstrip('/')}/{stem}_{timestamp}{suffix}"
                if duplicate_prefix
                else f"{stem}_{timestamp}{suffix}"
            )
        except ResourceNotFoundError:
            # File doesn't exist, use original name
            pass

        try:
            source_blob_client = blob_service_client.get_blob_client(
                container=source_container, blob=source_blob
            )
            dest_blob_client = blob_service_client.get_blob_client(
                container=duplicate_container, blob=destination_blob
            )
            # Copy then delete (Azure doesn't have move)
            dest_blob_client.start_copy_from_url(source_blob_client.url)
            source_blob_client.delete_blob()
        except Exception as e:
            raise FileMoveError(
                f"Failed to move Azure Blob from {file_path} to {duplicate_uri}/{destination_blob.split('/')[-1]}: {e}"
            )

    @classmethod
    @retry()
    def delete_file(cls, file_path: Union[Path, str]) -> None:
        """Delete Azure Blob."""
        if isinstance(file_path, Path):
            raise ValueError("AzureFileHelper requires Azure Blob URI, not local Path")

        _, container, blob_name = cls._parse_azure_uri(str(file_path))
        blob_service_client = cls._get_blob_service_client()
        try:
            blob_client = blob_service_client.get_blob_client(
                container=container, blob=blob_name
            )
            blob_client.delete_blob()
        except Exception as e:
            raise FileDeleteError(f"Failed to delete Azure Blob {file_path}: {e}")

    @classmethod
    def get_file_path(
        cls, directory_path: Union[Path, str], filename: str
    ) -> Union[Path, str]:
        """Construct Azure Blob URI from directory and filename."""
        if isinstance(directory_path, Path):
            raise ValueError("AzureFileHelper requires Azure Blob URI, not local Path")

        directory_uri = str(directory_path).rstrip("/")
        return f"{directory_uri}/{filename}"

    @classmethod
    @contextmanager
    @retry()
    def get_file_stream(cls, file_path: Union[Path, str], mode: str = "rb"):
        """Get streaming download from Azure Blob Storage."""
        if isinstance(file_path, Path):
            raise ValueError("AzureFileHelper requires Azure Blob URI, not local Path")

        if mode != "rb":
            raise ValueError("Azure Blob streams are always binary, mode must be 'rb'")

        _, container, blob_name = cls._parse_azure_uri(str(file_path))
        blob_service_client = cls._get_blob_service_client()

        try:
            blob_client = blob_service_client.get_blob_client(
                container=container, blob=blob_name
            )
            download_stream = blob_client.download_blob()
            yield download_stream
        except Exception as e:
            if "BlobNotFound" in str(e) or "404" in str(e):
                raise FileNotFoundError(f"Azure Blob not found: {file_path}")
            raise IOError(f"Failed to stream Azure Blob {file_path}: {e}")
