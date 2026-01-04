"""Delta table health analysis and recommendations."""

from dataclasses import dataclass
from typing import List, Dict, Any
from datetime import datetime, timedelta

from deltective.inspector import TableStatistics


@dataclass
class Insight:
    """A single insight about the Delta table."""

    severity: str  # "critical", "warning", "info", "good"
    category: str  # "performance", "cost", "maintenance", "reliability"
    title: str
    description: str
    recommendation: str


class DeltaTableAnalyzer:
    """Analyzes Delta tables for configuration issues and provides recommendations."""

    # Thresholds for analysis
    SMALL_FILE_THRESHOLD_MB = 10  # Files smaller than this are considered small
    OPTIMAL_FILE_SIZE_MB = 128  # Optimal file size
    MAX_RECOMMENDED_FILES = 1000  # Max files before recommend OPTIMIZE
    MIN_FILE_SIZE_VARIANCE = 0.5  # Coefficient of variation threshold for skew
    VACUUM_RECOMMENDATION_DAYS = 7  # Recommend vacuum if not done in X days
    MANY_SMALL_WRITES_THRESHOLD = 10  # Number of sequential small writes

    def __init__(self, stats: TableStatistics):
        """Initialize analyzer with table statistics."""
        self.stats = stats
        self.insights: List[Insight] = []

    def analyze(self) -> List[Insight]:
        """Run all analyses and return insights."""
        self.insights = []

        self._analyze_file_sizes()
        self._analyze_file_count()
        self._analyze_vacuum_history()
        self._analyze_partitioning()
        self._analyze_optimization_history()
        self._analyze_data_skew()
        self._analyze_write_patterns()

        # Add positive feedback if no issues found
        if not any(i.severity in ["critical", "warning"] for i in self.insights):
            self.insights.append(
                Insight(
                    severity="good",
                    category="performance",
                    title="Table Configuration Looks Good",
                    description="No significant configuration issues detected.",
                    recommendation="Continue monitoring the table as data grows.",
                )
            )

        # Sort by severity (critical first, then warning, info, good)
        severity_order = {"critical": 0, "warning": 1, "info": 2, "good": 3}
        self.insights.sort(key=lambda x: severity_order.get(x.severity, 99))

        return self.insights

    def _analyze_file_sizes(self):
        """Analyze file sizes for small files problem."""
        if not self.stats.files:
            return

        file_sizes_mb = [f.size_bytes / (1024 * 1024) for f in self.stats.files]
        avg_size_mb = sum(file_sizes_mb) / len(file_sizes_mb)
        small_files = [s for s in file_sizes_mb if s < self.SMALL_FILE_THRESHOLD_MB]

        if small_files:
            pct_small = (len(small_files) / len(file_sizes_mb)) * 100

            if pct_small > 50:
                self.insights.append(
                    Insight(
                        severity="critical",
                        category="performance",
                        title="Small Files Problem Detected",
                        description=f"{pct_small:.1f}% of files ({len(small_files)}/{len(file_sizes_mb)}) "
                        f"are smaller than {self.SMALL_FILE_THRESHOLD_MB}MB. "
                        f"Average file size: {avg_size_mb:.2f}MB. "
                        f"Small files severely impact query performance.",
                        recommendation="Run OPTIMIZE command to compact small files. "
                        f"Target file size is ~{self.OPTIMAL_FILE_SIZE_MB}MB. "
                        "Consider using Auto Optimize for future writes.",
                    )
                )
            elif pct_small > 20:
                self.insights.append(
                    Insight(
                        severity="warning",
                        category="performance",
                        title="Some Small Files Detected",
                        description=f"{pct_small:.1f}% of files are smaller than {self.SMALL_FILE_THRESHOLD_MB}MB. "
                        f"Average file size: {avg_size_mb:.2f}MB.",
                        recommendation="Consider running OPTIMIZE to improve performance. "
                        "Monitor file sizes and run OPTIMIZE periodically.",
                    )
                )

        # Check if average file size is far from optimal
        if avg_size_mb < self.OPTIMAL_FILE_SIZE_MB / 2:
            self.insights.append(
                Insight(
                    severity="warning",
                    category="performance",
                    title="Suboptimal Average File Size",
                    description=f"Average file size ({avg_size_mb:.2f}MB) is much smaller than "
                    f"optimal ({self.OPTIMAL_FILE_SIZE_MB}MB).",
                    recommendation="Run OPTIMIZE to compact files to optimal size. "
                    "Configure Auto Optimize for future writes.",
                )
            )

    def _analyze_file_count(self):
        """Analyze total file count."""
        if self.stats.num_files > self.MAX_RECOMMENDED_FILES:
            self.insights.append(
                Insight(
                    severity="warning",
                    category="performance",
                    title="High File Count",
                    description=f"Table has {self.stats.num_files:,} files. "
                    f"Recommended maximum is ~{self.MAX_RECOMMENDED_FILES:,} files. "
                    "High file count increases metadata overhead and slows queries.",
                    recommendation="Run OPTIMIZE to reduce file count. "
                    "Consider using Auto Optimize and adjusting partition strategy.",
                )
            )

    def _analyze_vacuum_history(self):
        """Analyze vacuum history."""
        if self.stats.last_vacuum is None:
            # Table has never been vacuumed
            if self.stats.total_versions > 10:
                self.insights.append(
                    Insight(
                        severity="warning",
                        category="cost",
                        title="Table Has Never Been Vacuumed",
                        description=f"Table has {self.stats.total_versions} versions but has never been vacuumed. "
                        "Old data files are accumulating, increasing storage costs.",
                        recommendation="Run VACUUM command to remove old data files. "
                        "Set up periodic VACUUM jobs (weekly or monthly). "
                        "Note: VACUUM deletes old versions permanently.",
                    )
                )
        else:
            # Check if vacuum is overdue
            days_since_vacuum = (datetime.now() - self.stats.last_vacuum).days
            if days_since_vacuum > self.VACUUM_RECOMMENDATION_DAYS * 4:  # 28 days
                self.insights.append(
                    Insight(
                        severity="warning",
                        category="cost",
                        title="Vacuum Overdue",
                        description=f"Last vacuum was {days_since_vacuum} days ago. "
                        "Old data files may be accumulating.",
                        recommendation=f"Run VACUUM to clean up old files. "
                        f"Recommended vacuum frequency: every {self.VACUUM_RECOMMENDATION_DAYS} days.",
                    )
                )

    def _analyze_partitioning(self):
        """Analyze partitioning strategy."""
        if not self.stats.partition_columns:
            # No partitioning
            if self.stats.total_size_bytes > 1024 * 1024 * 1024 * 10:  # > 10GB
                self.insights.append(
                    Insight(
                        severity="info",
                        category="performance",
                        title="Table Not Partitioned",
                        description=f"Table is {self._format_bytes(self.stats.total_size_bytes)} "
                        "but has no partitioning. Partitioning can improve query performance "
                        "by enabling partition pruning.",
                        recommendation="Consider partitioning by frequently filtered columns "
                        "(e.g., date, region, category). Avoid over-partitioning (too many partitions).",
                    )
                )
        else:
            # Analyze partition distribution
            if self.stats.files:
                # Count files per partition
                partition_counts: Dict[str, int] = {}
                for file in self.stats.files:
                    partition_key = str(sorted(file.partition_values.items()))
                    partition_counts[partition_key] = partition_counts.get(partition_key, 0) + 1

                num_partitions = len(partition_counts)
                avg_files_per_partition = self.stats.num_files / num_partitions if num_partitions > 0 else 0

                # Too many partitions (each with few files)
                if num_partitions > 1000 and avg_files_per_partition < 5:
                    self.insights.append(
                        Insight(
                            severity="warning",
                            category="performance",
                            title="Over-Partitioned Table",
                            description=f"Table has {num_partitions:,} partitions with average "
                            f"{avg_files_per_partition:.1f} files per partition. "
                            "Too many partitions creates excessive metadata overhead.",
                            recommendation="Consider coarser partitioning strategy "
                            "(e.g., partition by month instead of day). "
                            "Alternatively, use Z-ordering instead of partitioning.",
                        )
                    )

                # Too few partitions (many files each)
                elif num_partitions < 10 and avg_files_per_partition > 100:
                    self.insights.append(
                        Insight(
                            severity="info",
                            category="performance",
                            title="Under-Partitioned Table",
                            description=f"Table has only {num_partitions} partition(s) with "
                            f"{avg_files_per_partition:.0f} files per partition on average. "
                            "More granular partitioning could improve query performance.",
                            recommendation="Consider finer-grained partitioning if queries "
                            "frequently filter on specific columns.",
                        )
                    )

    def _analyze_optimization_history(self):
        """Analyze optimization history from operations."""
        # This would require checking the history for OPTIMIZE operations
        # For now, we can infer from version count and file patterns
        if self.stats.total_versions > 20:
            # Check if there's evidence of optimization
            # If there are many versions but files aren't optimized, recommend it
            if self.stats.num_files > self.MAX_RECOMMENDED_FILES:
                self.insights.append(
                    Insight(
                        severity="info",
                        category="maintenance",
                        title="Consider Regular Optimization",
                        description=f"Table has {self.stats.total_versions} versions and "
                        f"{self.stats.num_files:,} files. Regular optimization can maintain performance.",
                        recommendation="Set up periodic OPTIMIZE jobs (weekly or after major writes). "
                        "Enable Auto Optimize for automatic compaction.",
                    )
                )

    def _analyze_data_skew(self):
        """Analyze data skew by looking at file size variance."""
        if len(self.stats.files) < 2:
            return

        file_sizes = [f.size_bytes for f in self.stats.files]
        mean_size = sum(file_sizes) / len(file_sizes)
        variance = sum((s - mean_size) ** 2 for s in file_sizes) / len(file_sizes)
        std_dev = variance ** 0.5
        coef_variation = std_dev / mean_size if mean_size > 0 else 0

        if coef_variation > self.MIN_FILE_SIZE_VARIANCE:
            min_size = min(file_sizes)
            max_size = max(file_sizes)
            self.insights.append(
                Insight(
                    severity="warning",
                    category="performance",
                    title="Data Skew Detected",
                    description=f"High variance in file sizes detected (CV: {coef_variation:.2f}). "
                    f"File sizes range from {self._format_bytes(min_size)} to {self._format_bytes(max_size)}. "
                    "This indicates data skew which can cause uneven processing.",
                    recommendation="Run OPTIMIZE to balance file sizes. "
                    "Consider using Z-ordering or different partitioning strategy. "
                    "Review data distribution in partition columns.",
                )
            )

    def _analyze_write_patterns(self):
        """Analyze write patterns from operation history."""
        # Count recent small writes
        # This would require full history analysis
        # For now, we can provide general guidance based on file count vs versions
        if self.stats.total_versions > 1:
            files_per_version = self.stats.num_files / self.stats.total_versions
            if files_per_version < 5 and self.stats.total_versions > 10:
                self.insights.append(
                    Insight(
                        severity="info",
                        category="performance",
                        title="Many Small Writes Detected",
                        description=f"Table has {self.stats.total_versions} versions with "
                        f"~{files_per_version:.1f} files added per write on average. "
                        "Frequent small writes create many small files.",
                        recommendation="Batch writes together when possible. "
                        "Enable Auto Optimize to automatically compact small files. "
                        "Consider using Delta's MERGE operation for incremental updates.",
                    )
                )

    def _format_bytes(self, bytes_value: int) -> str:
        """Format bytes into human-readable string."""
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if bytes_value < 1024.0:
                return f"{bytes_value:.2f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.2f} PB"
