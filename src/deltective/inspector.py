"""Delta table inspection logic."""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pyarrow as pa
from deltalake import DeltaTable

try:
    from azure.identity import DefaultAzureCredential
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False


@dataclass
class FileInfo:
    """Information about a single file in the Delta table."""

    path: str
    size_bytes: int
    modification_time: datetime
    partition_values: Dict[str, str]


@dataclass
class TableStatistics:
    """Statistics about a Delta table."""

    table_path: str
    version: int
    num_files: int
    total_size_bytes: int
    schema: Dict[str, Any]
    partition_columns: List[str]
    num_rows: Optional[int]
    files: List[FileInfo]
    metadata: Dict[str, Any]
    # Delta-specific information
    total_versions: int
    oldest_version: int
    min_reader_version: int
    min_writer_version: int
    reader_features: List[str]
    writer_features: List[str]
    created_time: Optional[datetime]
    last_operation: Optional[Dict[str, Any]]
    last_vacuum: Optional[datetime]


class DeltaTableInspector:
    """Inspector for Delta Lake tables."""

    def __init__(self, table_path: str):
        """Initialize the inspector with a Delta table path.
        
        Supports local paths and Azure storage URLs (abfss://).
        For Azure storage, uses DefaultAzureCredential for authentication.
        """
        self.table_path = table_path
        # Store credential for Azure paths to enable token refresh
        self._azure_credential = None
        storage_options = self._get_storage_options(table_path)
        try:
            self.table = DeltaTable(table_path, storage_options=storage_options)
        except Exception as e:
            # Provide more helpful error messages for Azure authentication issues
            if table_path.startswith("abfss://") or table_path.startswith("az://"):
                error_msg = str(e)
                if "403" in error_msg or "Forbidden" in error_msg or "not authorized" in error_msg.lower():
                    raise RuntimeError(
                        f"Azure authentication failed with 403 Forbidden. "
                        f"This usually means your Azure identity doesn't have the required permissions. "
                        f"\n\nTo fix this:\n"
                        f"1. Ensure your Azure identity has 'Storage Blob Data Reader' role on the storage account\n"
                        f"2. Verify you're using the correct identity (check with 'az account show')\n"
                        f"3. If using a service principal, ensure it has the correct RBAC permissions\n"
                        f"\nOriginal error: {e}"
                    ) from e
                elif "401" in error_msg or "Unauthorized" in error_msg:
                    raise RuntimeError(
                        f"Azure authentication failed. The access token may have expired or be invalid. "
                        f"\n\nTo fix this:\n"
                        f"1. Re-authenticate with 'az login'\n"
                        f"2. If using environment variables, ensure they are set correctly\n"
                        f"3. Check that your Azure credentials are valid\n"
                        f"\nOriginal error: {e}"
                    ) from e
            raise
    
    def _get_storage_options(self, table_path: str) -> Optional[Dict[str, Any]]:
        """Get storage options for the given table path.

        For Azure storage paths (abfss://), configures authentication using
        Azure Default Credentials. Stores the credential object to enable
        token refresh if needed.
        """
        if table_path.startswith("abfss://") or table_path.startswith("az://"):
            if not AZURE_AVAILABLE:
                raise ImportError(
                    "Azure support requires the 'azure-identity' package. "
                    "Install it with: pip install azure-identity"
                )
            # Use Azure Default Credentials
            credential = DefaultAzureCredential()
            # Store credential for potential token refresh
            self._azure_credential = credential
            # Get an access token for Azure Storage
            # Use the correct scope for Azure Storage
            try:
                token = credential.get_token("https://storage.azure.com/.default")
            except Exception as e:
                raise RuntimeError(
                    f"Failed to obtain Azure access token. "
                    f"Ensure you are authenticated (e.g., 'az login' or set environment variables). "
                    f"Original error: {e}"
                ) from e

            # Extract account name from ABFSS URL
            # Format: abfss://container@account.dfs.core.windows.net/path
            account_name = self._extract_azure_account_name(table_path)

            storage_options = {
                "bearer_token": token.token,
                "use_fabric_endpoint": "false",
            }

            # Add account_name if we could extract it from the URL
            if account_name:
                storage_options["account_name"] = account_name

            return storage_options
        return None

    def _extract_azure_account_name(self, table_path: str) -> Optional[str]:
        """Extract the Azure storage account name from an ABFSS or AZ URL.

        Supports formats:
        - abfss://container@account.dfs.core.windows.net/path
        - abfss://container@account.blob.core.windows.net/path
        - az://container/path (account name not in URL)
        """
        import re

        if table_path.startswith("abfss://"):
            # Format: abfss://container@account.dfs.core.windows.net/path
            # or: abfss://container@account.blob.core.windows.net/path
            match = re.match(r"abfss://[^@]+@([^.]+)\.", table_path)
            if match:
                return match.group(1)

        # For az:// URLs, account name is typically not in the URL
        # and would need to be provided separately or via environment variables
        return None

    def get_statistics(self) -> TableStatistics:
        """Collect and return statistics about the Delta table."""
        # Get basic table information
        version = self.table.version()
        schema = self.get_schema_dict()
        metadata = self.table.metadata()

        # Get partition columns
        partition_columns = metadata.partition_columns if metadata else []

        # Get file information
        files_info = []
        total_size = 0

        # Convert arro3 RecordBatch to PyArrow RecordBatch
        add_actions_batch = pa.record_batch(self.table.get_add_actions(flatten=True))
        add_actions_dict = add_actions_batch.to_pydict()

        # Get number of files
        num_files = len(add_actions_dict["path"])

        for i in range(num_files):
            size = add_actions_dict.get("size_bytes", [0])[i] or 0
            total_size += size

            # Extract partition values from partition.* columns
            partition_values = {}
            for key in add_actions_dict.keys():
                if key.startswith("partition."):
                    partition_col = key.replace("partition.", "")
                    partition_val = add_actions_dict[key][i]
                    if partition_val is not None:
                        partition_values[partition_col] = str(partition_val)

            file_info = FileInfo(
                path=add_actions_dict["path"][i],
                size_bytes=size,
                modification_time=datetime.fromtimestamp(
                    add_actions_dict.get("modification_time", [0])[i] / 1000
                ),
                partition_values=partition_values,
            )
            files_info.append(file_info)

        # Try to get row count (may not always be available)
        num_rows = None
        try:
            # This will read the table to count rows - can be slow for large tables
            # For now, we'll skip this and rely on metadata if available
            pass
        except Exception:
            pass

        # Get Delta-specific information
        protocol = self.table.protocol()
        history = self.table.history()

        # Get version information
        total_versions = len(history)

        # Get protocol versions and features
        min_reader_version = protocol.min_reader_version
        min_writer_version = protocol.min_writer_version
        reader_features = list(protocol.reader_features) if protocol.reader_features else []
        writer_features = list(protocol.writer_features) if protocol.writer_features else []

        # Get created time from metadata
        created_time = None
        if metadata and metadata.created_time:
            created_time = datetime.fromtimestamp(metadata.created_time / 1000)

        # Get last operation from history
        last_operation = None
        if history:
            last_operation = {
                "operation": history[0].get("operation"),
                "timestamp": datetime.fromtimestamp(history[0].get("timestamp", 0) / 1000),
                "parameters": history[0].get("operationParameters"),
                "metrics": history[0].get("operationMetrics"),
            }

        # Check for last vacuum operation
        last_vacuum = None
        for entry in history:
            if entry.get("operation") == "VACUUM":
                last_vacuum = datetime.fromtimestamp(entry.get("timestamp", 0) / 1000)
                break

        # Get oldest available version (minimum version in history)
        oldest_version = 0
        if history:
            oldest_version = min(entry.get("version", 0) for entry in history)

        return TableStatistics(
            table_path=self.table_path,
            version=version,
            num_files=len(files_info),
            total_size_bytes=total_size,
            schema=schema,
            partition_columns=partition_columns,
            num_rows=num_rows,
            files=files_info,
            metadata={
                "id": str(metadata.id) if metadata else None,
                "name": metadata.name if metadata else None,
                "description": metadata.description if metadata else None,
                "created_time": metadata.created_time if metadata else None,
            },
            total_versions=total_versions,
            oldest_version=oldest_version,
            min_reader_version=min_reader_version,
            min_writer_version=min_writer_version,
            reader_features=reader_features,
            writer_features=writer_features,
            created_time=created_time,
            last_operation=last_operation,
            last_vacuum=last_vacuum,
        )

    def get_schema_dict(self) -> Dict[str, str]:
        """Get schema as a dictionary of column name to type."""
        schema = self.table.schema()
        # The schema object has a to_arrow() method that returns a PyArrow schema
        pyarrow_schema = schema.to_arrow()
        result = {}
        for field in pyarrow_schema:
            type_str = str(field.type).strip()
            # Clean up arro3 type representation
            if "DataType<" in type_str and type_str.endswith(">"):
                type_str = type_str.split("DataType<")[1][:-1]
            result[field.name] = type_str
        return result

    def get_history(self, reverse: bool = False) -> List[Dict[str, Any]]:
        """Get the full history of operations on the Delta table.

        Args:
            reverse: If True, returns oldest operations first. Default is newest first.

        Returns:
            List of operation dictionaries with timestamp, operation type, parameters, etc.
        """
        history = self.table.history()
        if reverse:
            return list(reversed(history))
        return history

    def get_configuration(self) -> Dict[str, Any]:
        """Get table configuration and properties.

        Returns:
            Dictionary with configuration details including properties, checkpoints, etc.
        """
        metadata = self.table.metadata()
        protocol = self.table.protocol()

        # Get table properties/configuration
        table_config = metadata.configuration if metadata else {}

        # Get checkpoint information
        # Transaction log analysis
        import os
        from pathlib import Path

        table_path = Path(self.table_path)
        delta_log_path = table_path / "_delta_log"

        checkpoint_info = {}
        transaction_log_info = {}

        if delta_log_path.exists():
            # Count transaction log files
            json_files = list(delta_log_path.glob("*.json"))
            checkpoint_files = list(delta_log_path.glob("*.checkpoint.parquet"))

            transaction_log_info = {
                "num_json_files": len(json_files),
                "num_checkpoints": len(checkpoint_files),
                "log_size_bytes": sum(f.stat().st_size for f in json_files),
            }

            if checkpoint_files:
                latest_checkpoint = max(checkpoint_files, key=lambda f: f.stat().st_mtime)
                checkpoint_info = {
                    "has_checkpoints": True,
                    "latest_checkpoint": latest_checkpoint.name,
                    "checkpoint_size_bytes": latest_checkpoint.stat().st_size,
                }
            else:
                checkpoint_info = {"has_checkpoints": False}

        return {
            "table_properties": dict(table_config),
            "table_id": str(metadata.id) if metadata else None,
            "table_name": metadata.name if metadata else None,
            "description": metadata.description if metadata else None,
            "created_time": metadata.created_time if metadata else None,
            "partition_columns": metadata.partition_columns if metadata else [],
            "protocol": {
                "min_reader_version": protocol.min_reader_version,
                "min_writer_version": protocol.min_writer_version,
                "reader_features": list(protocol.reader_features) if protocol.reader_features else [],
                "writer_features": list(protocol.writer_features) if protocol.writer_features else [],
            },
            "checkpoint_info": checkpoint_info,
            "transaction_log": transaction_log_info,
            "advanced_features": self._detect_advanced_features(table_config, protocol),
        }

    def _detect_advanced_features(self, config: Dict[str, str], protocol) -> Dict[str, Any]:
        """Detect advanced Delta features enabled on the table."""
        features = {}

        # Deletion Vectors (Delta 2.0+)
        features["deletion_vectors"] = "deletionVectors" in (protocol.writer_features or [])

        # Column Mapping
        column_mapping_mode = config.get("delta.columnMapping.mode", "none")
        features["column_mapping"] = {
            "enabled": column_mapping_mode != "none",
            "mode": column_mapping_mode,
        }

        # Liquid Clustering (Delta 3.0+)
        features["liquid_clustering"] = "clustering" in config

        # Timestamp without timezone
        features["timestamp_ntz"] = "timestampNtz" in (protocol.writer_features or [])

        # Constraints
        check_constraints = {k: v for k, v in config.items() if k.startswith("delta.constraints.")}
        features["check_constraints"] = check_constraints

        # Auto Optimize
        features["auto_optimize"] = {
            "enabled": config.get("delta.autoOptimize.autoCompact", "false") == "true"
            or config.get("delta.autoOptimize.optimizeWrite", "false") == "true",
            "auto_compact": config.get("delta.autoOptimize.autoCompact", "false") == "true",
            "optimize_write": config.get("delta.autoOptimize.optimizeWrite", "false") == "true",
        }

        # Data skipping (always enabled, but check for customization)
        features["data_skipping"] = {
            "enabled": True,
            "num_indexed_cols": int(config.get("delta.dataSkippingNumIndexedCols", "32")),
        }

        # Change Data Feed
        features["change_data_feed"] = config.get("delta.enableChangeDataFeed", "false") == "true"

        # Vacuum retention
        features["vacuum_retention_hours"] = int(config.get("delta.deletedFileRetentionDuration", "168").replace("hours", "").strip())

        return features

    def get_timeline_analysis(self) -> Dict[str, Any]:
        """Analyze operations over time and detect patterns.

        Returns:
            Dictionary with timeline analysis including operation frequency, patterns, etc.
        """
        history = self.table.history()

        if not history:
            return {
                "total_operations": 0,
                "operations_by_type": {},
                "operations_by_day": {},
                "version_creation_rate": 0,
                "write_patterns": [],
            }

        # Group operations by type
        operations_by_type: Dict[str, int] = {}
        for entry in history:
            op_type = entry.get("operation", "UNKNOWN")
            operations_by_type[op_type] = operations_by_type.get(op_type, 0) + 1

        # Group operations by day
        operations_by_day: Dict[str, List[Dict]] = {}
        for entry in history:
            timestamp = datetime.fromtimestamp(entry.get("timestamp", 0) / 1000)
            day_key = timestamp.strftime("%Y-%m-%d")
            if day_key not in operations_by_day:
                operations_by_day[day_key] = []
            operations_by_day[day_key].append(entry)

        # Calculate version creation rate (versions per day)
        if history:
            first_op = min(history, key=lambda x: x.get("timestamp", 0))
            last_op = max(history, key=lambda x: x.get("timestamp", 0))

            first_time = datetime.fromtimestamp(first_op.get("timestamp", 0) / 1000)
            last_time = datetime.fromtimestamp(last_op.get("timestamp", 0) / 1000)

            days_elapsed = max(1, (last_time - first_time).days or 1)
            version_creation_rate = len(history) / days_elapsed
        else:
            version_creation_rate = 0

        # Analyze write patterns
        write_patterns = self._analyze_write_patterns(history)

        return {
            "total_operations": len(history),
            "operations_by_type": operations_by_type,
            "operations_by_day": operations_by_day,
            "version_creation_rate": version_creation_rate,
            "write_patterns": write_patterns,
            "first_operation": history[-1] if history else None,
            "latest_operation": history[0] if history else None,
        }

    def _analyze_write_patterns(self, history: List[Dict]) -> List[str]:
        """Analyze write patterns and return observations."""
        patterns = []

        # Count write operations
        writes = [h for h in history if h.get("operation") in ["WRITE", "MERGE", "UPDATE", "DELETE"]]

        if not writes:
            return patterns

        # Detect small frequent writes
        if len(writes) > 10:
            avg_rows = sum(h.get("operationMetrics", {}).get("num_added_rows", 0) for h in writes) / len(writes)
            if avg_rows < 1000:
                patterns.append("Small frequent writes detected (avg < 1000 rows)")

        # Detect batch vs streaming
        timestamps = [h.get("timestamp", 0) for h in writes]
        if len(timestamps) > 1:
            time_diffs = [timestamps[i] - timestamps[i+1] for i in range(len(timestamps)-1)]
            avg_time_diff = sum(time_diffs) / len(time_diffs) / 1000  # Convert to seconds

            if avg_time_diff < 300:  # < 5 minutes
                patterns.append("Streaming pattern: writes every few minutes")
            elif avg_time_diff > 86400:  # > 1 day
                patterns.append("Batch pattern: writes once per day or less")

        return patterns

    def get_schema_evolution(self) -> Dict[str, Any]:
        """Track schema changes over time.

        Returns:
            Dictionary with schema evolution information including additions, removals, type changes.
        """
        history = self.table.history()
        schema_changes = []

        # We'll track schema through history by looking at the current schema
        # and comparing with what we can infer from history
        # Note: This is limited without actual schema tracking in transaction log

        # For now, we'll provide a basic implementation
        # In a full implementation, we'd need to read each version's schema

        current_schema = self.get_schema_dict()

        return {
            "current_schema": current_schema,
            "total_columns": len(current_schema),
            "schema_changes": schema_changes,
            "note": "Full schema evolution tracking requires reading each version's schema from transaction log",
        }
