import logging

logger = logging.getLogger(__name__)


class GCPBlobStreamWrapper:
    """File-like wrapper for GCS blob stream that tracks download progress."""

    def __init__(self, blob_stream):
        self.blob_stream = blob_stream
        self._closed = False
        self._bytes_downloaded = 0
        self._last_logged_mb = 0

    def read(self, size=-1):
        """Read bytes from the stream, tracking progress."""
        if self._closed:
            raise ValueError("I/O operation on closed file")

        data = self.blob_stream.read(size)
        if data:
            self._log_progress(len(data))
        return data

    def read1(self, size=-1):
        """Read bytes from the stream (used by buffered readers), tracking progress."""
        if self._closed:
            raise ValueError("I/O operation on closed file")

        if hasattr(self.blob_stream, "read1"):
            data = self.blob_stream.read1(size)
        else:
            data = self.blob_stream.read(size)
        if data:
            self._log_progress(len(data))
        return data

    def readinto(self, b):
        """Read bytes into a buffer, tracking progress."""
        if self._closed:
            raise ValueError("I/O operation on closed file")

        bytes_read = self.blob_stream.readinto(b)
        if bytes_read is not None and bytes_read > 0:
            self._log_progress(bytes_read)
        return bytes_read

    def _log_progress(self, bytes_read: int):
        """Log download progress every 4MB."""
        self._bytes_downloaded += bytes_read
        current_mb = self._bytes_downloaded / (1024 * 1024)
        if current_mb >= self._last_logged_mb + 4:
            logger.debug(f"Downloaded Total: {current_mb:.2f} MB from GCS")
            self._last_logged_mb = int(current_mb // 4) * 4

    def readable(self):
        return True

    def writable(self):
        return False

    def seekable(self):
        return False

    @property
    def closed(self):
        """Property expected by TextIOWrapper."""
        return self._closed

    def close(self):
        if not self._closed:
            if self._bytes_downloaded > 0:
                total_mb = self._bytes_downloaded / (1024 * 1024)
                logger.info(f"Finished downloading {total_mb:.2f} MB from GCS")
            else:
                logger.info("Finished downloading 0.00 MB from GCS")
        self._closed = True
        if hasattr(self.blob_stream, "close"):
            self.blob_stream.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __getattr__(self, name):
        """Delegate other attributes to the underlying blob stream."""
        # Don't delegate read methods - use our wrapped versions
        if name == "readinto":
            return self.readinto
        if name == "read1":
            return self.read1
        return getattr(self.blob_stream, name)
