"""Integration tests for DeltaTableInspector."""

from pathlib import Path

import pytest
from deltective.inspector import DeltaTableInspector, TableStatistics


class TestDeltaTableInspector:
    """Test DeltaTableInspector functionality."""

    def test_inspector_initialization(self, temp_delta_table: Path):
        """Test that inspector can be initialized with a Delta table."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        assert inspector is not None
        assert inspector.table_path == str(temp_delta_table)
        assert inspector.table is not None

    def test_get_statistics_basic(self, simple_delta_table: Path):
        """Test getting basic statistics from a simple Delta table."""
        inspector = DeltaTableInspector(str(simple_delta_table))
        stats = inspector.get_statistics()

        assert isinstance(stats, TableStatistics)
        assert stats.table_path == str(simple_delta_table)
        assert stats.version >= 0
        assert stats.num_files > 0
        assert stats.total_size_bytes > 0
        assert len(stats.schema) > 0
        assert stats.total_versions > 0

    def test_get_statistics_partitioned(self, temp_delta_table: Path):
        """Test statistics for a partitioned Delta table."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        stats = inspector.get_statistics()

        assert len(stats.partition_columns) == 2
        assert "country" in stats.partition_columns
        assert "department" in stats.partition_columns
        assert stats.num_files > 0

    def test_get_statistics_with_history(self, delta_table_with_history: Path):
        """Test statistics for a table with multiple versions."""
        inspector = DeltaTableInspector(str(delta_table_with_history))
        stats = inspector.get_statistics()

        assert stats.total_versions >= 10  # We created 11 versions
        assert stats.oldest_version == 0
        assert stats.version >= 10

    def test_schema_extraction(self, temp_delta_table: Path):
        """Test schema extraction."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        schema = inspector.get_schema_dict()

        assert isinstance(schema, dict)
        assert "id" in schema
        assert "name" in schema
        assert "age" in schema
        assert "country" in schema

    def test_get_history(self, delta_table_with_history: Path):
        """Test getting table history."""
        inspector = DeltaTableInspector(str(delta_table_with_history))
        history = inspector.get_history()

        assert isinstance(history, list)
        assert len(history) >= 10
        # History should be newest first by default
        assert history[0]["version"] > history[-1]["version"]

    def test_get_history_reverse(self, delta_table_with_history: Path):
        """Test getting table history in reverse order."""
        inspector = DeltaTableInspector(str(delta_table_with_history))
        history = inspector.get_history(reverse=True)

        assert isinstance(history, list)
        # History should be oldest first when reversed
        assert history[0]["version"] < history[-1]["version"]

    def test_get_configuration(self, temp_delta_table: Path):
        """Test getting table configuration."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        config = inspector.get_configuration()

        assert isinstance(config, dict)
        assert "table_properties" in config
        assert "protocol" in config
        assert "checkpoint_info" in config
        assert "transaction_log" in config
        assert "advanced_features" in config

        # Check protocol info
        protocol = config["protocol"]
        assert "min_reader_version" in protocol
        assert "min_writer_version" in protocol

    def test_get_timeline_analysis(self, delta_table_with_history: Path):
        """Test timeline analysis."""
        inspector = DeltaTableInspector(str(delta_table_with_history))
        timeline = inspector.get_timeline_analysis()

        assert isinstance(timeline, dict)
        assert "total_operations" in timeline
        assert "operations_by_type" in timeline
        assert "operations_by_day" in timeline
        assert "version_creation_rate" in timeline
        assert "write_patterns" in timeline

        assert timeline["total_operations"] > 0
        assert isinstance(timeline["operations_by_type"], dict)

    def test_get_schema_evolution(self, temp_delta_table: Path):
        """Test schema evolution tracking."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        evolution = inspector.get_schema_evolution()

        assert isinstance(evolution, dict)
        assert "current_schema" in evolution
        assert "total_columns" in evolution
        assert evolution["total_columns"] > 0

    def test_file_information(self, temp_delta_table: Path):
        """Test that file information is collected correctly."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        stats = inspector.get_statistics()

        assert len(stats.files) > 0
        for file_info in stats.files:
            assert file_info.path is not None
            assert file_info.size_bytes >= 0
            assert file_info.modification_time is not None
            # Partitioned table should have partition values
            assert isinstance(file_info.partition_values, dict)

    def test_metadata_extraction(self, temp_delta_table: Path):
        """Test metadata extraction."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        stats = inspector.get_statistics()

        assert stats.metadata is not None
        assert "name" in stats.metadata
        assert stats.metadata["name"] == "test_table"
        assert "description" in stats.metadata
        assert stats.metadata["description"] == "Test Delta table for Deltective"

    def test_protocol_versions(self, temp_delta_table: Path):
        """Test protocol version extraction."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        stats = inspector.get_statistics()

        assert stats.min_reader_version >= 0
        assert stats.min_writer_version >= 0
        assert isinstance(stats.reader_features, list)
        assert isinstance(stats.writer_features, list)

    def test_oldest_version_calculation(self, delta_table_with_history: Path):
        """Test oldest version is calculated correctly."""
        inspector = DeltaTableInspector(str(delta_table_with_history))
        stats = inspector.get_statistics()

        assert stats.oldest_version == 0
        assert stats.oldest_version <= stats.version

    def test_advanced_features_detection(self, temp_delta_table: Path):
        """Test advanced features detection."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        config = inspector.get_configuration()

        features = config["advanced_features"]
        assert isinstance(features, dict)
        assert "deletion_vectors" in features
        assert "column_mapping" in features
        assert "auto_optimize" in features
        assert "data_skipping" in features
        assert "change_data_feed" in features
        assert "vacuum_retention_hours" in features

        # Check structure of nested features
        assert isinstance(features["column_mapping"], dict)
        assert "enabled" in features["column_mapping"]
        assert isinstance(features["auto_optimize"], dict)
        assert "enabled" in features["auto_optimize"]


class TestAzureAccountNameExtraction:
    """Test Azure account name extraction from URLs."""

    def test_extract_account_name_abfss_dfs(self, temp_delta_table: Path):
        """Test extracting account name from abfss:// URL with dfs endpoint."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        url = "abfss://container@myaccount.dfs.core.windows.net/path/to/table"
        account_name = inspector._extract_azure_account_name(url)
        assert account_name == "myaccount"

    def test_extract_account_name_abfss_blob(self, temp_delta_table: Path):
        """Test extracting account name from abfss:// URL with blob endpoint."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        url = "abfss://container@storageacct.blob.core.windows.net/path"
        account_name = inspector._extract_azure_account_name(url)
        assert account_name == "storageacct"

    def test_extract_account_name_abfss_complex(self, temp_delta_table: Path):
        """Test extracting account name from real-world abfss:// URL."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        url = "abfss://landing-zone@stpdatalake44dk98.dfs.core.windows.net/sharepoint/cea_country_mapping"
        account_name = inspector._extract_azure_account_name(url)
        assert account_name == "stpdatalake44dk98"

    def test_extract_account_name_az_url(self, temp_delta_table: Path):
        """Test that az:// URLs return None (no account name in URL)."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        url = "az://container/path/to/table"
        account_name = inspector._extract_azure_account_name(url)
        assert account_name is None

    def test_extract_account_name_local_path(self, temp_delta_table: Path):
        """Test that local paths return None."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        url = "/local/path/to/table"
        account_name = inspector._extract_azure_account_name(url)
        assert account_name is None
