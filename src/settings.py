import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import Optional, Union

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from pythonnet import load

from src.utils import aws_secret_helper, azure_secret_helper, gcp_secret_helper

logger = logging.getLogger(__name__)
SUPPORTED_DATABASE_DRIVERS = {
    "postgresql": "postgresql",
    "mysql": "mysql",
    "mssql": "mssql",
    "sqlite": "sqlite",
    "bigquery": "bigquery",
}


class BaseConfig(BaseSettings):
    ENV_STATE: Optional[str] = None

    @classmethod
    def _get_secret_field_mapping(cls):
        return {
            "aws": [],
            "azure": [],
            "gcp": [],
        }

    @model_validator(mode="before")
    @classmethod
    def resolve_secrets(cls, data: dict):
        resolved = {}
        secret_mapping = cls._get_secret_field_mapping()
        for field_name, value in data.items():
            if not value or not isinstance(value, str):
                resolved[field_name] = value
                continue

            if field_name in secret_mapping.get("aws", []):
                resolved[field_name] = aws_secret_helper(value)
            elif field_name in secret_mapping.get("azure", []):
                resolved[field_name] = azure_secret_helper(value)
            elif field_name in secret_mapping.get("gcp", []):
                resolved[field_name] = gcp_secret_helper(value)
            else:
                resolved[field_name] = value

        return resolved

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class GlobalConfig(BaseConfig):
    DATABASE_URL: str
    DIRECTORY_PATH: Union[Path, str]
    ARCHIVE_PATH: Union[Path, str]
    DUPLICATE_FILES_PATH: Union[Path, str]

    BATCH_SIZE: int = 100000
    LOG_LEVEL: str = "INFO"

    @property
    def DRIVERNAME(self) -> str:
        for drivername, dialect in SUPPORTED_DATABASE_DRIVERS.items():
            if drivername in self.DATABASE_URL.lower():
                return dialect.lower()
        raise ValueError(
            f"Unsupported database driver in DATABASE_URL: {self.DATABASE_URL}"
        )

    # Email notification settings
    SMTP_HOST: Optional[str] = None
    SMTP_PORT: Optional[int] = 587
    SMTP_USER: Optional[str] = None
    SMTP_PASSWORD: Optional[str] = None
    FROM_EMAIL: Optional[str] = None
    DATA_TEAM_EMAIL: Optional[str] = None  # Always CC'd on failure notifications
    # Generic webhook notification settings (works with Slack, MS Teams, etc.)
    WEBHOOK_URL: Optional[str] = None

    # AWS S3 settings
    AWS_ACCESS_KEY_ID: Optional[str] = None
    AWS_SECRET_ACCESS_KEY: Optional[str] = None
    AWS_SESSION_TOKEN: Optional[str] = None  # For temporary credentials
    AWS_REGION: Optional[str] = None  # Defaults to boto3's default region chain

    # Azure Blob Storage settings
    AZURE_STORAGE_CONNECTION_STRING: Optional[str] = None
    AZURE_STORAGE_ACCOUNT_URL: Optional[str] = None
    AZURE_STORAGE_ACCOUNT_KEY: Optional[str] = None

    # Azure Key Vault settings (for secret manager access)
    AZURE_CLIENT_ID: Optional[str] = None
    AZURE_CLIENT_SECRET: Optional[str] = None
    AZURE_TENANT_ID: Optional[str] = None
    AZURE_KEY_VAULT_URL: Optional[str] = None

    # GCP Cloud Storage settings
    GOOGLE_APPLICATION_CREDENTIALS: Optional[str] = (
        None  # Path to service account JSON file
    )
    # For GCP Secret Manager access
    GOOGLE_CLOUD_PROJECT: Optional[str] = None

    @field_validator(
        "DIRECTORY_PATH", "ARCHIVE_PATH", "DUPLICATE_FILES_PATH", mode="before"
    )
    @classmethod
    def convert_path(cls, v):
        if isinstance(v, Path):
            return v
        v_str = str(v)
        if v_str.startswith(("s3://", "gs://", "azure://", "https://")):
            return v_str
        return Path(v_str)

    OTEL_PYTHON_LOG_CORRELATION: Optional[bool] = None
    OPEN_TELEMETRY_LOG_ENDPOINT: Optional[str] = None
    OPEN_TELEMETRY_TRACE_ENDPOINT: Optional[str] = None
    OPEN_TELEMETRY_AUTHORIZATION_TOKEN: Optional[str] = None
    OPEN_TELEMETRY_FLAG: bool = False

    SQL_SERVER_SQLBULKCOPY_FLAG: bool = False

    FILE_HELPER_PLATFORM: str = "default"

    @field_validator("FILE_HELPER_PLATFORM", mode="before")
    @classmethod
    def lowercase_file_helper_platform(cls, v):
        if v is None:
            return "default"
        v_lower = v.lower()
        valid_platforms = {"default", "aws", "gcp", "azure"}
        if v_lower not in valid_platforms:
            raise ValueError(
                f"FILE_HELPER_PLATFORM must be one of {valid_platforms}, got: {v}"
            )
        return v_lower


class DevConfig(GlobalConfig):
    DIRECTORY_PATH: Union[Path, str] = Path("src/tests/test_directory")
    ARCHIVE_PATH: Union[Path, str] = Path("src/tests/test_archive")
    DUPLICATE_FILES_PATH: Union[Path, str] = Path("src/tests/test_duplicate_files")
    LOG_LEVEL: str = "DEBUG"
    OTEL_PYTHON_LOG_CORRELATION: bool = False

    model_config = SettingsConfigDict(env_prefix="DEV_")


class TestConfig(GlobalConfig):
    DATABASE_URL: str = "sqlite:///:memory:"
    DIRECTORY_PATH: Union[Path, str] = Path("src/tests/test_data")
    ARCHIVE_PATH: Union[Path, str] = Path("src/tests/archive_data")
    DUPLICATE_FILES_PATH: Union[Path, str] = Path("src/tests/duplicate_files_data")
    BATCH_SIZE: int = 100
    OTEL_PYTHON_LOG_CORRELATION: bool = False

    model_config = SettingsConfigDict(env_prefix="TEST_")


class ProdConfig(GlobalConfig):
    LOG_LEVEL: Optional[str] = "WARNING"
    OTEL_PYTHON_LOG_CORRELATION: bool = True
    OPEN_TELEMETRY_FLAG: bool = True

    model_config = SettingsConfigDict(env_prefix="PROD_")


@lru_cache()
def get_config(env_state: str):
    if not env_state:
        raise ValueError("ENV_STATE is not set. Possible values are: DEV, TEST, PROD")
    env_state = env_state.lower()

    # Set up environment variables for secret manager authentication BEFORE creating config
    # This is needed because resolve_secrets() runs during config initialization
    # Only set authentication credentials for DEV - PROD uses IAM/Managed Identity/ADC
    if env_state == "dev":
        prefix = env_state.upper() + "_"

        aws_access_key_id = os.environ.get(f"{prefix}AWS_ACCESS_KEY_ID")
        if aws_access_key_id:
            os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key_id
        aws_secret_access_key = os.environ.get(f"{prefix}AWS_SECRET_ACCESS_KEY")
        if aws_secret_access_key:
            os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key
        aws_session_token = os.environ.get(f"{prefix}AWS_SESSION_TOKEN")
        if aws_session_token:
            os.environ["AWS_SESSION_TOKEN"] = aws_session_token
        aws_region = os.environ.get(f"{prefix}AWS_REGION")
        if aws_region:
            os.environ["AWS_REGION"] = aws_region
        gcp_creds = os.environ.get(f"{prefix}GOOGLE_APPLICATION_CREDENTIALS")
        if gcp_creds:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_creds
        azure_client_id = os.environ.get(f"{prefix}AZURE_CLIENT_ID")
        if azure_client_id:
            os.environ["AZURE_CLIENT_ID"] = azure_client_id
        azure_client_secret = os.environ.get(f"{prefix}AZURE_CLIENT_SECRET")
        if azure_client_secret:
            os.environ["AZURE_CLIENT_SECRET"] = azure_client_secret
        azure_tenant_id = os.environ.get(f"{prefix}AZURE_TENANT_ID")
        if azure_tenant_id:
            os.environ["AZURE_TENANT_ID"] = azure_tenant_id
        azure_vault_url = os.environ.get(f"{prefix}AZURE_KEY_VAULT_URL")
        if azure_vault_url:
            os.environ["AZURE_KEY_VAULT_URL"] = azure_vault_url

    configs = {"dev": DevConfig, "prod": ProdConfig, "test": TestConfig}
    config_instance = configs[env_state]()

    return config_instance


config = get_config(BaseConfig().ENV_STATE)


def _initialize_dotnet_runtime():
    """Initialize .NET runtime once at startup if using SQL Server and bulk copy is enabled."""
    if config.DRIVERNAME == "mssql" and config.SQL_SERVER_SQLBULKCOPY_FLAG:
        try:
            runtime = os.environ.get("PYTHONNET_RUNTIME", "coreclr")
            load(runtime)
            logger.debug(f"Initialized .NET runtime: {runtime}")
        except Exception as e:
            # Log but don't fail, will handle the error when actually used
            logger.warning(f"Failed to initialize .NET runtime at startup: {e}")


# Initialize .NET runtime at module load if needed
_initialize_dotnet_runtime()


def get_database_config():
    env_state = BaseConfig().ENV_STATE
    db_config = get_config(env_state)

    config_dict = {
        "sqlalchemy.url": db_config.DATABASE_URL,
        "sqlalchemy.echo": False,
        "sqlalchemy.future": True,
    }

    if config.DRIVERNAME == "sqlite":
        config_dict["sqlalchemy.connect_args"] = {"check_same_thread": False}
        config_dict["sqlalchemy.pool_size"] = 1
    else:
        config_dict["sqlalchemy.pool_size"] = 20
        config_dict["sqlalchemy.max_overflow"] = 10
        config_dict["sqlalchemy.pool_timeout"] = 30

    return config_dict
