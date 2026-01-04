"""Unit tests for utility functions."""

from datetime import datetime
from pathlib import Path

import pytest
from deltective.inspector import DeltaTableInspector


class TestSchemaFormatting:
    """Test schema extraction and formatting."""

    def test_get_schema_dict_returns_dict(self, simple_delta_table: Path):
        """Test that get_schema_dict returns a dictionary."""
        inspector = DeltaTableInspector(str(simple_delta_table))
        schema = inspector.get_schema_dict()

        assert isinstance(schema, dict)

    def test_schema_dict_has_correct_columns(self, simple_delta_table: Path):
        """Test that schema dict contains expected columns."""
        inspector = DeltaTableInspector(str(simple_delta_table))
        schema = inspector.get_schema_dict()

        assert "id" in schema
        assert "value" in schema
        assert "label" in schema

    def test_schema_dict_has_string_types(self, simple_delta_table: Path):
        """Test that schema dict values are string type names."""
        inspector = DeltaTableInspector(str(simple_delta_table))
        schema = inspector.get_schema_dict()

        for col_name, col_type in schema.items():
            assert isinstance(col_type, str)
            assert len(col_type) > 0


class TestTimestampHandling:
    """Test timestamp conversion and formatting."""

    def test_creation_time_conversion(self, temp_delta_table: Path):
        """Test that creation time is properly converted from milliseconds."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        stats = inspector.get_statistics()

        if stats.created_time:
            assert isinstance(stats.created_time, datetime)
            # Should be a reasonable date (after 2020, before 2030)
            assert stats.created_time.year >= 2020
            assert stats.created_time.year < 2030

    def test_operation_timestamp_conversion(self, delta_table_with_history: Path):
        """Test that operation timestamps are properly converted."""
        inspector = DeltaTableInspector(str(delta_table_with_history))
        stats = inspector.get_statistics()

        if stats.last_operation:
            timestamp = stats.last_operation.get("timestamp")
            assert isinstance(timestamp, datetime)
            assert timestamp.year >= 2020


class TestPartitionValueExtraction:
    """Test partition value extraction from files."""

    def test_partition_values_extracted(self, temp_delta_table: Path):
        """Test that partition values are extracted for partitioned tables."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        stats = inspector.get_statistics()

        # Partitioned table should have partition values in file info
        for file in stats.files:
            if file.partition_values:
                # Partition values should be strings
                for key, value in file.partition_values.items():
                    assert isinstance(key, str)
                    assert isinstance(value, str)

    def test_non_partitioned_table_empty_partition_values(self, simple_delta_table: Path):
        """Test that non-partitioned tables have empty partition values."""
        inspector = DeltaTableInspector(str(simple_delta_table))
        stats = inspector.get_statistics()

        # Non-partitioned table should have empty partition values
        assert len(stats.partition_columns) == 0
        for file in stats.files:
            # Partition values dict should be empty or all None
            if file.partition_values:
                assert len(file.partition_values) == 0 or all(
                    v is None for v in file.partition_values.values()
                )


class TestVersionCalculations:
    """Test version-related calculations."""

    def test_total_versions_matches_history_length(self, delta_table_with_history: Path):
        """Test that total_versions matches history length."""
        inspector = DeltaTableInspector(str(delta_table_with_history))
        stats = inspector.get_statistics()
        history = inspector.get_history()

        assert stats.total_versions == len(history)

    def test_oldest_version_is_minimum(self, delta_table_with_history: Path):
        """Test that oldest_version is the minimum version in history."""
        inspector = DeltaTableInspector(str(delta_table_with_history))
        stats = inspector.get_statistics()
        history = inspector.get_history()

        if history:
            min_version = min(entry.get("version", 0) for entry in history)
            assert stats.oldest_version == min_version

    def test_current_version_is_maximum(self, delta_table_with_history: Path):
        """Test that current version is the maximum version."""
        inspector = DeltaTableInspector(str(delta_table_with_history))
        stats = inspector.get_statistics()
        history = inspector.get_history()

        if history:
            max_version = max(entry.get("version", 0) for entry in history)
            assert stats.version == max_version


class TestFileSizeCalculations:
    """Test file size calculations and aggregation."""

    def test_total_size_is_sum_of_files(self, temp_delta_table: Path):
        """Test that total_size_bytes equals sum of all file sizes."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        stats = inspector.get_statistics()

        calculated_total = sum(file.size_bytes for file in stats.files)
        assert stats.total_size_bytes == calculated_total

    def test_all_files_have_positive_size(self, temp_delta_table: Path):
        """Test that all files have non-negative size."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        stats = inspector.get_statistics()

        for file in stats.files:
            assert file.size_bytes >= 0

    def test_file_count_matches_files_list(self, temp_delta_table: Path):
        """Test that num_files matches length of files list."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        stats = inspector.get_statistics()

        assert stats.num_files == len(stats.files)


class TestHistoryProcessing:
    """Test history processing and ordering."""

    def test_history_default_order_newest_first(self, delta_table_with_history: Path):
        """Test that history is ordered newest first by default."""
        inspector = DeltaTableInspector(str(delta_table_with_history))
        history = inspector.get_history(reverse=False)

        if len(history) > 1:
            for i in range(len(history) - 1):
                current_version = history[i].get("version", 0)
                next_version = history[i + 1].get("version", 0)
                assert current_version >= next_version

    def test_history_reverse_order_oldest_first(self, delta_table_with_history: Path):
        """Test that history can be reversed to oldest first."""
        inspector = DeltaTableInspector(str(delta_table_with_history))
        history = inspector.get_history(reverse=True)

        if len(history) > 1:
            for i in range(len(history) - 1):
                current_version = history[i].get("version", 0)
                next_version = history[i + 1].get("version", 0)
                assert current_version <= next_version


class TestAdvancedFeaturesDetection:
    """Test advanced Delta features detection."""

    def test_column_mapping_structure(self, temp_delta_table: Path):
        """Test column mapping detection structure."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        config = inspector.get_configuration()

        col_mapping = config["advanced_features"]["column_mapping"]
        assert isinstance(col_mapping, dict)
        assert "enabled" in col_mapping
        assert "mode" in col_mapping
        assert isinstance(col_mapping["enabled"], bool)

    def test_auto_optimize_structure(self, temp_delta_table: Path):
        """Test auto optimize detection structure."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        config = inspector.get_configuration()

        auto_opt = config["advanced_features"]["auto_optimize"]
        assert isinstance(auto_opt, dict)
        assert "enabled" in auto_opt
        assert "auto_compact" in auto_opt
        assert "optimize_write" in auto_opt

    def test_data_skipping_structure(self, temp_delta_table: Path):
        """Test data skipping detection structure."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        config = inspector.get_configuration()

        data_skip = config["advanced_features"]["data_skipping"]
        assert isinstance(data_skip, dict)
        assert "enabled" in data_skip
        assert "num_indexed_cols" in data_skip

    def test_vacuum_retention_is_number(self, temp_delta_table: Path):
        """Test vacuum retention is extracted as a number."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        config = inspector.get_configuration()

        retention = config["advanced_features"]["vacuum_retention_hours"]
        assert isinstance(retention, int)
        assert retention > 0
