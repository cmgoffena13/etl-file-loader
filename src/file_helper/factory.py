from src.file_helper.aws_file_helper import AWSFileHelper
from src.file_helper.azure_file_helper import AzureFileHelper
from src.file_helper.base import BaseFileHelper
from src.file_helper.file_helper import FileHelper
from src.file_helper.gcp_file_helper import GCPFileHelper
from src.settings import config


class FileHelperFactory:
    _file_helpers = {
        "default": FileHelper,
        "azure": AzureFileHelper,
        "aws": AWSFileHelper,
        "gcp": GCPFileHelper,
    }

    @classmethod
    def get_supported_platforms(cls) -> list[str]:
        return list[str](cls._file_helpers.keys())

    @classmethod
    def create_file_helper(cls) -> type[BaseFileHelper]:
        try:
            file_helper_class = cls._file_helpers[config.FILE_HELPER_PLATFORM]
        except KeyError:
            raise ValueError(
                f"Unsupported platform for file helper: {config.FILE_HELPER_PLATFORM}. Supported platforms: {cls.get_supported_platforms()}"
            )
        return file_helper_class
