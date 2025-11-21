import logging
import platform
import socket
import sys
from pathlib import Path
from typing import Any, Dict, Iterator
from urllib.parse import parse_qs, unquote, urlparse

from sqlalchemy import Engine, Table

from src.pipeline.write.base import BaseWriter
from src.settings import config
from src.sources.base import DataSource

logger = logging.getLogger(__name__)


class SQLServerWriter(BaseWriter):
    def __init__(
        self,
        source: DataSource,
        engine: Engine,
        file_load_dlq_table: Table,
        log_id: int,
        stage_table_name: str,
    ):
        super().__init__(source, engine, file_load_dlq_table, log_id, stage_table_name)
        self.column_count = (
            len(self.source.source_model.model_fields) + 2
        )  # +2 for etl_row_hash and source_filename
        self.max_rows_from_values = (
            2100 // self.column_count
        ) - 1  # -1 for safety margin
        self.max_rows_from_records = 1000
        self.max_rows = max(
            1, min(self.max_rows_from_values, self.max_rows_from_records)
        )
        self.batch_size = (
            config.BATCH_SIZE if config.SQL_SERVER_SQLBULKCOPY_FLAG else self.max_rows
        )
        self.file_load_dlq_table_name = file_load_dlq_table.name

    def _get_runtime_platform_identifier(self) -> str:
        """Determine the .NET runtime platform identifier.

        Returns platform identifier like 'linux-x64', 'linux-arm64', 'win-x64', 'osx-arm64', etc.
        """
        system = platform.system().lower()  # e.g., 'windows', 'linux', 'darwin'
        architecture = platform.machine().lower()  # e.g., 'x86_64', 'aarch64'

        # Based on https://github.com/pythonnet/pythonnet/discussions/2307
        # Mapping of system and architecture to custom platform names
        if system in ["windows", "windows_nt"] and architecture == "amd64":
            return "win-x64"
        elif system == "linux" and architecture in [
            "arm",
            "arm64",
            "aarch64_b",
            "aarch64",
            "armv8b",
            "armv8l",
        ]:
            return "linux-arm64"
        elif system == "darwin":
            return "osx-arm64"
        elif system == "linux":
            return "linux-x64"
        else:
            return "unknown"

    def _ensure_clr_available(self) -> None:
        """Lazily import pythonnet/.NET components, raising a clear error if unavailable."""
        try:
            # Runtime should already be loaded in settings.py at startup
            import clr
            import System  # type: ignore[import-untyped]

            clr.AddReference("System.Data.Common")
            DataTable = System.Data.DataTable
            DBNull = System.DBNull

            runtime = self._get_runtime_platform_identifier()
            dll_dir = (
                Path(__file__).parent.parent.parent / "net-runtime-specific" / runtime
            )

            if not dll_dir.exists():
                raise ImportError(
                    f"Platform-specific DLL folder not found: {dll_dir}. "
                    f"Runtime: {runtime}, System: {platform.system()}, Architecture: {platform.machine()}"
                )

            sys.path.insert(0, str(dll_dir))
            clr.AddReference("Microsoft.Data.SqlClient")

            from Microsoft.Data.SqlClient import (  # type: ignore[import-untyped]
                SqlBulkCopy,
                SqlConnection,
                SqlConnectionStringBuilder,
            )

            return (
                DataTable,
                DBNull,
                SqlBulkCopy,
                SqlConnection,
                SqlConnectionStringBuilder,
            )
        except ImportError as e:
            logger.exception(f"Failed to import .NET components: {e}")
            raise ImportError(
                f".NET Framework is required for SQL Server bulk operations. "
                f"Error: {e}. Ensure .NET Framework is available."
            ) from e
        except (SystemError, RuntimeError) as e:
            logger.exception(f"Failed to initialize .NET runtime: {e}")
            raise

    def _get_dotnet_conn_string(self) -> str:
        _, _, _, _, SqlConnectionStringBuilder = self._ensure_clr_available()
        url = config.DATABASE_URL.replace("mssql+pyodbc://", "mssql://")
        parsed = urlparse(url)

        username = unquote(parsed.username or "")
        password = unquote(parsed.password or "")
        host = parsed.hostname or ""
        port = parsed.port or 1433
        database = parsed.path.lstrip("/") if parsed.path else ""

        # Parse query parameters
        query_params = parse_qs(parsed.query)
        trust_cert = (
            query_params.get("TrustServerCertificate", ["no"])[0].lower() == "yes"
        )

        # Keep original hostname for certificate validation
        hostname_for_cert = host
        try:
            ip_address = socket.gethostbyname(host)
            server_address = f"{ip_address},{port}"
        except (socket.gaierror, OSError) as e:
            logger.exception(f"Failed to resolve hostname '{host}': {e}")
            server_address = f"{host},{port}"

        builder = SqlConnectionStringBuilder()
        builder.DataSource = server_address
        builder.InitialCatalog = database
        builder.UserID = username
        builder.Password = password
        builder["Encrypt"] = "True"
        builder.Pooling = False
        builder.ConnectTimeout = 30

        if trust_cert:
            builder["TrustServerCertificate"] = "True"
            # Set HostNameInCertificate to original hostname to avoid certificate name mismatch
            # when connecting via IP address
            builder["HostNameInCertificate"] = hostname_for_cert

        return builder.ConnectionString

    def _initialize_datatable_columns(self, sample_record: Dict[str, Any], dt):
        """Initialize DataTable columns from a sample record (only called once)."""
        _, _, _, _, _ = self._ensure_clr_available()
        import System  # type: ignore[import-untyped]

        column_types = {
            "etl_row_hash": System.Array[System.Byte],
            "file_load_log_id": System.Int64,
            "file_row_number": System.Int32,
        }

        for col in sample_record.keys():
            if col in column_types:
                column = System.Data.DataColumn(col, column_types[col])
                dt.Columns.Add(column)
            else:
                dt.Columns.Add(col)

    def _add_record_to_datatable(self, record: Dict[str, Any], dt):
        """Add a single record to the DataTable."""
        _, DBNull, _, _, _ = self._ensure_clr_available()
        import System  # type: ignore[import-untyped]

        column_types = {
            "etl_row_hash": System.Array[System.Byte],
            "file_load_log_id": System.Int64,
            "file_row_number": System.Int32,
        }

        dr = dt.NewRow()
        for key, value in record.items():
            if value is None:
                dr[key] = DBNull.Value
            elif key in column_types:
                col_type = column_types[key]
                if col_type == System.Array[System.Byte]:
                    # etl_row_hash: convert Python bytes to .NET byte array
                    dr[key] = System.Array[System.Byte](value)
                elif col_type == System.Int64:
                    dr[key] = System.Int64(value)
                elif col_type == System.Int32:
                    dr[key] = System.Int32(value)
                else:
                    dr[key] = value
            else:
                dr[key] = value
        dt.Rows.Add(dr)

    def bulk_write(
        self,
        batches: Iterator[tuple[bool, list[Dict[str, Any]]]],
    ) -> None:
        # DOTNET Setup and Connection
        DataTable, _, SqlBulkCopy, SqlConnection, _ = self._ensure_clr_available()
        dotnet_conn_string = self._get_dotnet_conn_string()
        conn = SqlConnection(dotnet_conn_string)

        valid_dt = DataTable()
        valid_dt.TableName = self.stage_table_name
        invalid_dt = DataTable()
        invalid_dt.TableName = self.file_load_dlq_table_name

        valid_bulk_copy = SqlBulkCopy(conn)
        valid_bulk_copy.DestinationTableName = self.stage_table_name
        invalid_bulk_copy = SqlBulkCopy(conn)
        invalid_bulk_copy.DestinationTableName = self.file_load_dlq_table_name

        valid_count = 0
        invalid_count = 0
        try:
            conn.Open()
            for batch in batches:
                for passed, record in batch:
                    if passed:
                        if valid_dt.Columns.Count == 0:
                            self._initialize_datatable_columns(record, valid_dt)

                        self._add_record_to_datatable(record, valid_dt)
                        valid_count += 1

                        if valid_count == self.batch_size:
                            valid_bulk_copy.WriteToServer(valid_dt)
                            self.rows_written_to_stage += valid_count
                            valid_dt.Clear()
                            valid_count = 0
                    else:
                        if invalid_dt.Columns.Count == 0:
                            self._initialize_datatable_columns(record, invalid_dt)

                        self._add_record_to_datatable(record, invalid_dt)
                        invalid_count += 1

                        if invalid_count == self.batch_size:
                            invalid_bulk_copy.WriteToServer(invalid_dt)
                            self.rows_written_to_stage += invalid_count
                            invalid_dt.Clear()
                            invalid_count = 0

            if valid_count > 0:
                valid_bulk_copy.WriteToServer(valid_dt)
                self.rows_written_to_stage += valid_count
            if invalid_count > 0:
                invalid_bulk_copy.WriteToServer(invalid_dt)
                self.rows_written_to_stage += invalid_count
        except Exception as e:
            logger.exception(
                f"[log_id={self.log_id}] Failed to SqlBulkCopy insert into {self.stage_table_name}: {e}"
            )
            raise
        finally:
            try:
                if conn.State == 1:  # ConnectionState.Open
                    conn.Close()
                conn.Dispose()
            except Exception as cleanup_error:
                logger.warning(
                    f"[log_id={self.log_id}] Error during connection cleanup: {cleanup_error}"
                )

    def write(
        self,
        batches: Iterator[tuple[bool, list[Dict[str, Any]]]],
    ) -> None:
        if config.SQL_SERVER_SQLBULKCOPY_FLAG:
            self.bulk_write(batches)
        else:
            super().write(batches)
