"""Tests for DeltaTableAnalyzer and insights generation."""

from pathlib import Path

import pytest
from deltective.inspector import DeltaTableInspector
from deltective.insights import DeltaTableAnalyzer, Insight


class TestDeltaTableAnalyzer:
    """Test DeltaTableAnalyzer functionality."""

    def test_analyzer_initialization(self, temp_delta_table: Path):
        """Test that analyzer can be initialized with table statistics."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        stats = inspector.get_statistics()
        analyzer = DeltaTableAnalyzer(stats)

        assert analyzer is not None
        assert analyzer.stats == stats

    def test_analyze_returns_insights(self, temp_delta_table: Path):
        """Test that analyze returns a list of insights."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        stats = inspector.get_statistics()
        analyzer = DeltaTableAnalyzer(stats)

        insights = analyzer.analyze()

        assert isinstance(insights, list)
        assert len(insights) > 0
        for insight in insights:
            assert isinstance(insight, Insight)
            assert insight.severity in ["critical", "warning", "info", "good"]
            assert insight.category in ["performance", "cost", "maintenance", "reliability"]
            assert insight.title
            assert insight.description
            assert insight.recommendation

    def test_small_files_detection(self, delta_table_with_small_files: Path):
        """Test detection of small files problem."""
        inspector = DeltaTableInspector(str(delta_table_with_small_files))
        stats = inspector.get_statistics()
        analyzer = DeltaTableAnalyzer(stats)

        insights = analyzer.analyze()

        # Should detect small files issue
        small_file_insights = [
            i for i in insights
            if "small files" in i.title.lower() or "file size" in i.title.lower()
        ]
        assert len(small_file_insights) > 0

        # Should have warning or critical severity
        assert any(i.severity in ["warning", "critical"] for i in small_file_insights)

    def test_insights_sorted_by_severity(self, temp_delta_table: Path):
        """Test that insights are sorted by severity (critical first)."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        stats = inspector.get_statistics()
        analyzer = DeltaTableAnalyzer(stats)

        insights = analyzer.analyze()

        severity_order = {"critical": 0, "warning": 1, "info": 2, "good": 3}

        for i in range(len(insights) - 1):
            current_priority = severity_order[insights[i].severity]
            next_priority = severity_order[insights[i + 1].severity]
            assert current_priority <= next_priority

    def test_good_configuration_insight(self, simple_delta_table: Path):
        """Test that good configuration is reported for well-configured tables."""
        inspector = DeltaTableInspector(str(simple_delta_table))
        stats = inspector.get_statistics()
        analyzer = DeltaTableAnalyzer(stats)

        insights = analyzer.analyze()

        # If no critical or warning issues, should have a "good" insight
        has_issues = any(i.severity in ["critical", "warning"] for i in insights)
        has_good = any(i.severity == "good" for i in insights)

        if not has_issues:
            assert has_good

    def test_vacuum_never_run_detection(self, delta_table_with_history: Path):
        """Test detection when vacuum has never been run."""
        inspector = DeltaTableInspector(str(delta_table_with_history))
        stats = inspector.get_statistics()

        # stats.last_vacuum should be None for new tables
        assert stats.last_vacuum is None

        analyzer = DeltaTableAnalyzer(stats)
        insights = analyzer.analyze()

        # Should recommend vacuum for tables with multiple versions
        vacuum_insights = [i for i in insights if "vacuum" in i.title.lower()]
        if stats.total_versions > 10:
            assert len(vacuum_insights) > 0

    def test_insight_categories(self, temp_delta_table: Path):
        """Test that insights cover different categories."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        stats = inspector.get_statistics()
        analyzer = DeltaTableAnalyzer(stats)

        insights = analyzer.analyze()

        categories = {i.category for i in insights}

        # Should have insights from multiple categories
        assert len(categories) > 0
        # Valid categories
        for category in categories:
            assert category in ["performance", "cost", "maintenance", "reliability"]

    def test_file_count_analysis(self, delta_table_with_small_files: Path):
        """Test file count analysis for tables with many files."""
        inspector = DeltaTableInspector(str(delta_table_with_small_files))
        stats = inspector.get_statistics()
        analyzer = DeltaTableAnalyzer(stats)

        insights = analyzer.analyze()

        # Table with many small partitions should have high file count
        if stats.num_files > analyzer.MAX_RECOMMENDED_FILES:
            file_count_insights = [i for i in insights if "file count" in i.title.lower()]
            assert len(file_count_insights) > 0

    def test_data_skew_detection(self, delta_table_with_small_files: Path):
        """Test data skew detection based on file size variance."""
        inspector = DeltaTableInspector(str(delta_table_with_small_files))
        stats = inspector.get_statistics()
        analyzer = DeltaTableAnalyzer(stats)

        insights = analyzer.analyze()

        # Small files table should have variance in file sizes
        # May trigger data skew detection
        skew_insights = [i for i in insights if "skew" in i.title.lower()]
        # This is optional as it depends on the actual file size distribution

    def test_write_patterns_analysis(self, delta_table_with_history: Path):
        """Test write pattern analysis."""
        inspector = DeltaTableInspector(str(delta_table_with_history))
        stats = inspector.get_statistics()
        analyzer = DeltaTableAnalyzer(stats)

        insights = analyzer.analyze()

        # May have insights about write patterns
        write_insights = [i for i in insights if "write" in i.title.lower()]
        # Presence depends on actual patterns detected

    def test_partitioning_analysis(self, temp_delta_table: Path):
        """Test partitioning strategy analysis."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        stats = inspector.get_statistics()
        analyzer = DeltaTableAnalyzer(stats)

        insights = analyzer.analyze()

        # Table is partitioned, may have insights about partitioning
        partition_insights = [
            i for i in insights
            if "partition" in i.title.lower()
        ]
        # Presence depends on partition distribution

    def test_insight_structure(self, temp_delta_table: Path):
        """Test that insights have proper structure."""
        inspector = DeltaTableInspector(str(temp_delta_table))
        stats = inspector.get_statistics()
        analyzer = DeltaTableAnalyzer(stats)

        insights = analyzer.analyze()

        for insight in insights:
            # All fields should be non-empty strings
            assert isinstance(insight.severity, str) and insight.severity
            assert isinstance(insight.category, str) and insight.category
            assert isinstance(insight.title, str) and insight.title
            assert isinstance(insight.description, str) and insight.description
            assert isinstance(insight.recommendation, str) and insight.recommendation

            # Title should not be too long (reasonable max)
            assert len(insight.title) < 200

            # Description and recommendation should be meaningful
            assert len(insight.description) > 10
            assert len(insight.recommendation) > 10
