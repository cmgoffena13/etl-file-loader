import logging

logger = logging.getLogger(__name__)


class AzureChunkedStreamReader:
    """File-like wrapper for Azure StorageStreamDownloader that reads chunks on demand."""

    def __init__(self, download_stream):
        self.download_stream = download_stream
        self._chunks = download_stream.chunks()
        self._buffer = b""
        self._closed = False
        self._bytes_downloaded = 0
        self._last_logged_mb = 0

    def _log_progress(self, bytes_read: int):
        """Log download progress every MB."""
        self._bytes_downloaded += bytes_read
        current_mb = self._bytes_downloaded / (1024 * 1024)
        if current_mb >= self._last_logged_mb + 1:
            logger.debug(f"Downloaded Total: {current_mb:.2f} MB from Azure Blob")
            self._last_logged_mb = int(current_mb)

    def read(self, size=-1):
        """Read bytes from the stream, fetching chunks as needed."""
        if self._closed:
            raise ValueError("I/O operation on closed file")

        if size == -1:
            result = self._buffer
            self._buffer = b""
            for chunk in self._chunks:
                result += chunk
                self._log_progress(len(chunk))
            return result

        while len(self._buffer) < size:
            try:
                chunk = next(self._chunks)
                self._buffer += chunk
                self._log_progress(len(chunk))
            except StopIteration:
                break

        if len(self._buffer) <= size:
            result = self._buffer
            self._buffer = b""
            return result
        else:
            result = self._buffer[:size]
            self._buffer = self._buffer[size:]
            return result

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
                logger.info(f"Finished downloading {total_mb:.2f} MB from Azure Blob")
            else:
                logger.info("Finished downloading 0.00 MB from Azure Blob")
        self._closed = True
        if hasattr(self.download_stream, "close"):
            self.download_stream.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
