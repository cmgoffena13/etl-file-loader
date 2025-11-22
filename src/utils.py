import logging
import os
import time
from functools import wraps
from pathlib import Path
from typing import Optional, Union

from src.exception.base import BaseFileErrorEmailException
from src.exception.exceptions import FileDeleteError

logger = logging.getLogger(__name__)


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
