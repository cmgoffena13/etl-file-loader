import os
import time
from functools import wraps
from pathlib import Path
from typing import Optional, Union

import boto3
import structlog
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from botocore.exceptions import ClientError
from google.cloud import secretmanager

from src.exception.base import BaseFileErrorEmailException

logger = structlog.getLogger(__name__)


def retry(attempts: int = 3, delay: float = 0.25, backoff: float = 2.0):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            wait = delay
            for i in range(attempts):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    # Don't retry file-specific validation errors
                    if isinstance(e, BaseFileErrorEmailException):
                        raise e

                    if i == attempts - 1:
                        raise e
                    logger.warning(
                        f"Retrying {fn.__name__} (attempt {i + 2}/{attempts}) after {type(e).__name__}: {e}"
                    )
                    time.sleep(wait)
                    wait *= backoff

        return wrapper

    return decorator


def get_error_location(exception: Exception) -> Optional[str]:
    if not exception.__traceback__:
        return None

    tb = exception.__traceback__
    while tb.tb_next:
        tb = tb.tb_next
    frame = tb.tb_frame
    filename = os.path.basename(frame.f_code.co_filename)
    return f"{filename}:{tb.tb_lineno}"


def get_file_name(file_path: Union[Path, str]) -> str:
    """Extract filename from Path object or URI string."""
    if isinstance(file_path, str):
        # For URI strings (e.g., s3://bucket/path/file.csv.gz), extract just the filename
        # Remove query parameters and fragments if present
        path_part = file_path.split("?")[0].split("#")[0]
        # Get the last part after the last slash
        return path_part.split("/")[-1]
    return file_path.name


def get_file_extension(file_path: Union[Path, str]) -> str:
    """Get file extension from Path object or URI string, handling .gz files."""
    if isinstance(file_path, str):
        path_part = file_path.split("?")[0].split("#")[0]
        filename = path_part.split("/")[-1]
        path_obj = Path(filename)
    else:
        path_obj = file_path

    suffixes = path_obj.suffixes

    if len(suffixes) >= 2:
        return "".join(suffixes[-2:]).lower()

    return path_obj.suffix.lower()


def aws_secret_helper(value: str) -> str:
    client = boto3.client("secretsmanager")
    try:
        response = client.get_secret_value(SecretId=value)
        secret_value = response.get("SecretString")
        if not secret_value:
            secret_binary = response.get("SecretBinary")
            if secret_binary:
                secret_value = secret_binary.decode("utf-8")
            else:
                raise ValueError(
                    f"AWS secret {value} has no SecretString or SecretBinary"
                )
        logger.debug(f"Fetched secret from AWS Secrets Manager: {value}")
        return secret_value
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code == "ResourceNotFoundException":
            logger.error(f"AWS secret not found: {value}")
            raise ValueError(f"AWS secret not found: {value}") from e
        logger.error(f"Error fetching AWS secret {value}: {e}")
        raise


def gcp_secret_helper(value: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    try:
        project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
        if not project_id:
            raise ValueError("GOOGLE_CLOUD_PROJECT environment variable must be set")

        name = f"projects/{project_id}/secrets/{value}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        secret_value = response.payload.data.decode("UTF-8")
        logger.debug(f"Fetched secret from GCP Secret Manager: {value}")
        return secret_value
    except Exception as e:
        logger.error(f"Error fetching GCP secret {value}: {e}")
        raise


def azure_secret_helper(value: str) -> str:
    vault_url = os.environ.get("AZURE_KEY_VAULT_URL")
    if not vault_url:
        raise ValueError("AZURE_KEY_VAULT_URL environment variable must be set")

    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=vault_url, credential=credential)
    try:
        secret_value = client.get_secret(value).value
        logger.debug(f"Fetched secret from Azure Key Vault: {value}")
        return secret_value
    except Exception as e:
        logger.error(f"Error fetching Azure secret {value}: {e}")
        raise
