import logging
import os
import time
from functools import wraps
from pathlib import Path
from typing import Optional

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


def delete_temp_file(file_path: Path) -> None:
    try:
        file_path.unlink()
    except FileNotFoundError:
        pass
    except Exception as e:
        raise FileDeleteError(f"Failed to delete local file {file_path}: {e}")


def get_error_location(exception: Exception) -> Optional[str]:
    if not exception.__traceback__:
        return None

    tb = exception.__traceback__
    while tb.tb_next:
        tb = tb.tb_next
    frame = tb.tb_frame
    filename = os.path.basename(frame.f_code.co_filename)
    return f"{filename}:{tb.tb_lineno}"


def get_file_extension(file_path: Path) -> str:
    suffixes = file_path.suffixes

    if len(suffixes) >= 2:
        return "".join(suffixes[-2:]).lower()

    return file_path.suffix.lower()
