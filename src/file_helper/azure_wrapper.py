import logging
from pathlib import Path
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class AdlfsFileWrapper:
    """File-like wrapper for adlfs file objects that tracks download progress."""

    def __init__(self, file_obj, file_path: str = None):
        self.file_obj = file_obj
        self._closed = False
        self._bytes_downloaded = 0
        self._last_logged_mb = 0
        self._filename = None

        if file_path:
            if file_path.startswith("https://") or file_path.startswith("azure://"):
                parsed = urlparse(file_path)
                self._filename = Path(parsed.path).name
            else:
                self._filename = Path(file_path).name
        else:
            self._filename = "unknown"

    def read(self, size=-1):
        if self._closed:
            raise ValueError("I/O operation on closed file")

        if hasattr(self.file_obj, "read"):
            data = self.file_obj.read(size)
        else:
            raise AttributeError("File object has no 'read' method")
        if data:
            self._log_progress(len(data))
        return data

    def readinto(self, b):
        if self._closed:
            raise ValueError("I/O operation on closed file")

        if hasattr(self.file_obj, "readinto"):
            bytes_read = self.file_obj.readinto(b)
        else:
            raise AttributeError("File object has no 'readinto' method")
        if bytes_read is not None and bytes_read > 0:
            self._log_progress(bytes_read)
        return bytes_read

    def read1(self, size=-1):
        """Read bytes from the stream (used by buffered readers), tracking progress."""
        if self._closed:
            raise ValueError("I/O operation on closed file")

        if hasattr(self.file_obj, "read1"):
            data = self.file_obj.read1(size)
        else:
            data = self.file_obj.read(size)
        if data:
            self._log_progress(len(data))
        return data

    def read_buffer(self, size=-1):
        """Read buffer method used by some libraries."""
        if self._closed:
            raise ValueError("I/O operation on closed file")

        if hasattr(self.file_obj, "read_buffer"):
            data = self.file_obj.read_buffer(size)
        else:
            data = self.file_obj.read(size)
        if data:
            self._log_progress(len(data))
        return data

    def _log_progress(self, bytes_read: int):
        """Log download progress every 4MB."""
        self._bytes_downloaded += bytes_read
        current_mb = self._bytes_downloaded / (1024 * 1024)
        if current_mb >= self._last_logged_mb + 4:
            logger.debug(
                f"Downloaded Total: {current_mb:.2f} MB from Azure Blob for file: {self._filename}"
            )
            self._last_logged_mb = int(current_mb // 4) * 4

    def readable(self):
        return True

    def writable(self):
        return False

    def seekable(self):
        return True

    def seek(self, pos, whence=0):
        return self.file_obj.seek(pos, whence)

    def tell(self):
        """Get current file position."""
        return self.file_obj.tell()

    @property
    def closed(self):
        """Property expected by TextIOWrapper."""
        return self._closed

    def close(self):
        if not self._closed:
            if self._bytes_downloaded > 0:
                total_mb = self._bytes_downloaded / (1024 * 1024)
                if total_mb >= 0.01:  # Show MB if >= 10KB
                    logger.info(
                        f"Finished downloading {total_mb:.2f} MB from Azure Blob for file: {self._filename}"
                    )
                else:
                    total_kb = self._bytes_downloaded / 1024
                    logger.info(
                        f"Finished downloading {total_kb:.2f} KB from Azure Blob for file: {self._filename}"
                    )
            else:
                logger.info(
                    f"Finished downloading 0.00 MB from Azure Blob for file: {self._filename}"
                )
        self._closed = True
        if hasattr(self.file_obj, "close"):
            self.file_obj.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __getattr__(self, name):
        """Delegate other attributes to the underlying file object."""
        # Return our wrapped methods if requested
        if name == "read":
            return self.read
        if name == "read1":
            return self.read1
        if name == "readinto":
            return self.readinto
        if name == "read_buffer":
            return self.read_buffer
        if name == "seek":
            return self.seek
        if name == "tell":
            return self.tell
        if name == "close":
            return self.close

        return getattr(self.file_obj, name)
