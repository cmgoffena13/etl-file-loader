import logging
import platform
import socket
import sys
from pathlib import Path
from typing import Any, Dict, Iterator
from urllib.parse import parse_qs, unquote, urlparse

from sqlalchemy import Engine, Table

from src.exception.base import BaseFileErrorEmailException
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
        self.file_load_dlq_table_name = str(file_load_dlq_table.name)

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
                SqlBulkCopyOptions,
                SqlConnection,
                SqlConnectionStringBuilder,
            )

            return (
                DataTable,
                DBNull,
                SqlBulkCopy,
                SqlBulkCopyOptions,
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
        _, _, _, _, _, SqlConnectionStringBuilder = self._ensure_clr_available()
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

    def _initialize_datatable_columns(
        self, sample_record: Dict[str, Any], dt, dotnet_types
    ):
        """Initialize DataTable columns from a sample record (only called once)."""
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

    def _convert_value_to_dotnet(self, key: str, value: Any, dotnet_types) -> Any:
        """Convert a Python value to the appropriate .NET type."""
        _, DBNull, _, _, _, _ = dotnet_types
        import System  # type: ignore[import-untyped]

        if value is None:
            return DBNull.Value

        column_types = {
            "etl_row_hash": System.Array[System.Byte],
            "file_load_log_id": System.Int64,
            "file_row_number": System.Int32,
        }

        if key in column_types:
            col_type = column_types[key]
            if col_type == System.Array[System.Byte]:
                # etl_row_hash: convert Python bytes to .NET byte array
                return System.Array[System.Byte](value)
            elif col_type == System.Int64:
                return System.Int64(value)
            elif col_type == System.Int32:
                return System.Int32(value)

        return value

    def _add_batch_to_datatable(
        self,
        records: list[Dict[str, Any]],
        dt,
        dotnet_types,
        column_order: list[str],
    ):
        """Add a batch of records to the DataTable using object arrays for better performance."""
        dt.BeginLoadData()
        for record in records:
            # Convert values to .NET types in column order
            values = [
                self._convert_value_to_dotnet(key, record[key], dotnet_types)
                for key in column_order
            ]
            dt.Rows.Add(values)
        dt.EndLoadData()

    def bulk_write(
        self,
        batches: Iterator[tuple[bool, list[Dict[str, Any]]]],
    ) -> None:
        # DOTNET Setup and Connection - get types once
        dotnet_types = self._ensure_clr_available()
        DataTable, _, SqlBulkCopy, SqlBulkCopyOptions, SqlConnection, _ = dotnet_types
        dotnet_conn_string = self._get_dotnet_conn_string()
        conn = SqlConnection(dotnet_conn_string)

        valid_dt = DataTable()
        valid_dt.TableName = self.stage_table_name
        valid_bulk_copy = SqlBulkCopy(conn)
        valid_bulk_copy.DestinationTableName = self.stage_table_name
        valid_bulk_copy.BulkCopyOptions = SqlBulkCopyOptions.TableLock

        invalid_dt = DataTable()
        invalid_dt.TableName = self.file_load_dlq_table_name
        invalid_bulk_copy = SqlBulkCopy(conn)
        invalid_bulk_copy.DestinationTableName = self.file_load_dlq_table_name
        invalid_bulk_copy.BulkCopyOptions = SqlBulkCopyOptions.TableLock

        valid_batch = [None] * self.batch_size
        valid_index = 0
        valid_column_order = None
        invalid_batch = []
        invalid_count = 0
        invalid_column_order = None
        try:
            conn.Open()
            for batch in batches:
                for passed, record in batch:
                    if passed:
                        if valid_dt.Columns.Count == 0:
                            self._initialize_datatable_columns(
                                record, valid_dt, dotnet_types
                            )
                            valid_column_order = [
                                col.ColumnName for col in valid_dt.Columns
                            ]

                        valid_batch[valid_index] = record
                        valid_index += 1

                        if valid_index == self.batch_size:
                            self._add_batch_to_datatable(
                                valid_batch[:valid_index],
                                valid_dt,
                                dotnet_types,
                                valid_column_order,
                            )
                            logger.debug(
                                f"[log_id={self.log_id}] Writing batch of {valid_index} rows to stage table {self.stage_table_name}"
                            )
                            valid_batch[:] = [None] * self.batch_size
                            valid_bulk_copy.WriteToServer(valid_dt)
                            self.rows_written_to_stage += valid_index
                            valid_index = 0
                            valid_dt.Clear()
                    else:
                        if invalid_dt.Columns.Count == 0:
                            self._initialize_datatable_columns(
                                record, invalid_dt, dotnet_types
                            )
                            invalid_column_order = [
                                col.ColumnName for col in invalid_dt.Columns
                            ]

                        invalid_batch.append(record)
                        invalid_count += 1

                        if invalid_count == self.batch_size:
                            self._add_batch_to_datatable(
                                invalid_batch,
                                invalid_dt,
                                dotnet_types,
                                invalid_column_order,
                            )
                            logger.debug(
                                f"[log_id={self.log_id}] Writing batch of {invalid_count} rows to dlq table {self.file_load_dlq_table_name}"
                            )
                            invalid_batch.clear()
                            invalid_bulk_copy.WriteToServer(invalid_dt)
                            self.rows_written_to_stage += invalid_count
                            invalid_count = 0
                            invalid_dt.Clear()
                if (
                    self.rows_written_to_stage % 100000 == 0
                    or self.rows_written_to_stage < 100000
                ) and self.rows_written_to_stage > 0:
                    logger.info(
                        f"[log_id={self.log_id}] Rows written: {self.rows_written_to_stage}"
                    )

            # Write final batches
            if valid_batch:
                valid_batch = valid_batch[:valid_index]
                self._add_batch_to_datatable(
                    valid_batch, valid_dt, dotnet_types, valid_column_order
                )
                logger.debug(
                    f"[log_id={self.log_id}] Writing final batch of {len(valid_batch)} rows to stage table {self.stage_table_name}"
                )
                valid_bulk_copy.WriteToServer(valid_dt)
                self.rows_written_to_stage += len(valid_batch)
            if invalid_batch:
                self._add_batch_to_datatable(
                    invalid_batch, invalid_dt, dotnet_types, invalid_column_order
                )
                logger.debug(
                    f"[log_id={self.log_id}] Writing final batch of {len(invalid_batch)} rows to dlq table {self.file_load_dlq_table_name}"
                )
                invalid_bulk_copy.WriteToServer(invalid_dt)
                self.rows_written_to_stage += len(invalid_batch)
        except BaseFileErrorEmailException:
            raise
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
