use crate::inspector::TableStatistics;
use chrono::Utc;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Insight {
    pub severity: String, // "critical", "warning", "info", "good"
    pub category: String, // "performance", "cost", "maintenance", "reliability"
    pub title: String,
    pub description: String,
    pub recommendation: String,
}

pub struct DeltaTableAnalyzer {
    stats: TableStatistics,
    insights: Vec<Insight>,
}

impl DeltaTableAnalyzer {
    const SMALL_FILE_THRESHOLD_MB: f64 = 10.0;
    const OPTIMAL_FILE_SIZE_MB: f64 = 128.0;
    const MAX_RECOMMENDED_FILES: usize = 1000;
    const MIN_FILE_SIZE_VARIANCE: f64 = 0.5;
    const VACUUM_RECOMMENDATION_DAYS: i64 = 7;

    pub fn new(stats: TableStatistics) -> Self {
        Self {
            stats,
            insights: Vec::new(),
        }
    }

    pub fn analyze(mut self) -> Vec<Insight> {
        self.insights.clear();

        self.analyze_file_sizes();
        self.analyze_file_count();
        self.analyze_vacuum_history();
        self.analyze_partitioning();
        self.analyze_optimization_history();
        self.analyze_data_skew();
        self.analyze_write_patterns();

        // Add positive feedback if no issues found
        if !self.insights.iter().any(|i| {
            i.severity == "critical" || i.severity == "warning"
        }) {
            self.insights.push(Insight {
                severity: "good".to_string(),
                category: "performance".to_string(),
                title: "Table Configuration Looks Good".to_string(),
                description: "No significant configuration issues detected.".to_string(),
                recommendation: "Continue monitoring the table as data grows.".to_string(),
            });
        }

        // Sort by severity
        let severity_order: std::collections::HashMap<&str, i32> = [
            ("critical", 0),
            ("warning", 1),
            ("info", 2),
            ("good", 3),
        ]
        .iter()
        .cloned()
        .collect();

        self.insights.sort_by_key(|x| {
            severity_order.get(x.severity.as_str()).copied().unwrap_or(99)
        });

        self.insights
    }

    fn analyze_file_sizes(&mut self) {
        if self.stats.files.is_empty() {
            return;
        }

        let file_sizes_mb: Vec<f64> = self
            .stats
            .files
            .iter()
            .map(|f| f.size_bytes as f64 / (1024.0 * 1024.0))
            .collect();

        let avg_size_mb = file_sizes_mb.iter().sum::<f64>() / file_sizes_mb.len() as f64;
        let small_files: Vec<f64> = file_sizes_mb
            .iter()
            .filter(|&&s| s < Self::SMALL_FILE_THRESHOLD_MB)
            .copied()
            .collect();

        if !small_files.is_empty() {
            let pct_small = (small_files.len() as f64 / file_sizes_mb.len() as f64) * 100.0;

            if pct_small > 50.0 {
                self.insights.push(Insight {
                    severity: "critical".to_string(),
                    category: "performance".to_string(),
                    title: "Small Files Problem Detected".to_string(),
                    description: format!(
                        "{:.1}% of files ({}/{}) are smaller than {}MB. Average file size: {:.2}MB. Small files severely impact query performance.",
                        pct_small,
                        small_files.len(),
                        file_sizes_mb.len(),
                        Self::SMALL_FILE_THRESHOLD_MB,
                        avg_size_mb
                    ),
                    recommendation: format!(
                        "Run OPTIMIZE command to compact small files. Target file size is ~{}MB. Consider using Auto Optimize for future writes.",
                        Self::OPTIMAL_FILE_SIZE_MB
                    ),
                });
            } else if pct_small > 20.0 {
                self.insights.push(Insight {
                    severity: "warning".to_string(),
                    category: "performance".to_string(),
                    title: "Some Small Files Detected".to_string(),
                    description: format!(
                        "{:.1}% of files are smaller than {}MB. Average file size: {:.2}MB.",
                        pct_small,
                        Self::SMALL_FILE_THRESHOLD_MB,
                        avg_size_mb
                    ),
                    recommendation: "Consider running OPTIMIZE to improve performance. Monitor file sizes and run OPTIMIZE periodically.".to_string(),
                });
            }
        }

        // Check if average file size is far from optimal
        if avg_size_mb < Self::OPTIMAL_FILE_SIZE_MB / 2.0 {
            self.insights.push(Insight {
                severity: "warning".to_string(),
                category: "performance".to_string(),
                title: "Suboptimal Average File Size".to_string(),
                description: format!(
                    "Average file size ({:.2}MB) is much smaller than optimal ({}MB).",
                    avg_size_mb,
                    Self::OPTIMAL_FILE_SIZE_MB
                ),
                recommendation: "Run OPTIMIZE to compact files to optimal size. Configure Auto Optimize for future writes.".to_string(),
            });
        }
    }

    fn analyze_file_count(&mut self) {
        if self.stats.num_files > Self::MAX_RECOMMENDED_FILES {
            self.insights.push(Insight {
                severity: "warning".to_string(),
                category: "performance".to_string(),
                title: "High File Count".to_string(),
                description: format!(
                    "Table has {} files. Recommended maximum is ~{} files. High file count increases metadata overhead and slows queries.",
                    self.stats.num_files,
                    Self::MAX_RECOMMENDED_FILES
                ),
                recommendation: "Run OPTIMIZE to reduce file count. Consider using Auto Optimize and adjusting partition strategy.".to_string(),
            });
        }
    }

    fn analyze_vacuum_history(&mut self) {
        if self.stats.last_vacuum.is_none() {
            if self.stats.total_versions > 10 {
                self.insights.push(Insight {
                    severity: "warning".to_string(),
                    category: "cost".to_string(),
                    title: "Table Has Never Been Vacuumed".to_string(),
                    description: format!(
                        "Table has {} versions but has never been vacuumed. Old data files are accumulating, increasing storage costs.",
                        self.stats.total_versions
                    ),
                    recommendation: "Run VACUUM command to remove old data files. Set up periodic VACUUM jobs (weekly or monthly). Note: VACUUM deletes old versions permanently.".to_string(),
                });
            }
        } else {
            let days_since_vacuum = (Utc::now() - self.stats.last_vacuum.unwrap())
                .num_days();
            if days_since_vacuum > Self::VACUUM_RECOMMENDATION_DAYS * 4 {
                self.insights.push(Insight {
                    severity: "warning".to_string(),
                    category: "cost".to_string(),
                    title: "Vacuum Overdue".to_string(),
                    description: format!(
                        "Last vacuum was {} days ago. Old data files may be accumulating.",
                        days_since_vacuum
                    ),
                    recommendation: format!(
                        "Run VACUUM to clean up old files. Recommended vacuum frequency: every {} days.",
                        Self::VACUUM_RECOMMENDATION_DAYS
                    ),
                });
            }
        }
    }

    fn analyze_partitioning(&mut self) {
        if self.stats.partition_columns.is_empty() {
            if self.stats.total_size_bytes > 10 * 1024 * 1024 * 1024 {
                self.insights.push(Insight {
                    severity: "info".to_string(),
                    category: "performance".to_string(),
                    title: "Table Not Partitioned".to_string(),
                    description: format!(
                        "Table is {} but has no partitioning. Partitioning can improve query performance by enabling partition pruning.",
                        Self::format_bytes(self.stats.total_size_bytes)
                    ),
                    recommendation: "Consider partitioning by frequently filtered columns (e.g., date, region, category). Avoid over-partitioning (too many partitions).".to_string(),
                });
            }
        } else {
            if !self.stats.files.is_empty() {
                use std::collections::HashMap;
                let mut partition_counts: HashMap<String, usize> = HashMap::new();
                for file in &self.stats.files {
                    let mut partition_parts: Vec<String> = file
                        .partition_values
                        .iter()
                        .map(|(k, v)| format!("{}={}", k, v))
                        .collect();
                    partition_parts.sort();
                    let partition_key = partition_parts.join(",");
                    *partition_counts.entry(partition_key).or_insert(0) += 1;
                }

                let num_partitions = partition_counts.len();
                let avg_files_per_partition =
                    self.stats.num_files as f64 / num_partitions as f64;

                // Too many partitions
                if num_partitions > 1000 && avg_files_per_partition < 5.0 {
                    self.insights.push(Insight {
                        severity: "warning".to_string(),
                        category: "performance".to_string(),
                        title: "Over-Partitioned Table".to_string(),
                        description: format!(
                            "Table has {} partitions with average {:.1} files per partition. Too many partitions creates excessive metadata overhead.",
                            num_partitions,
                            avg_files_per_partition
                        ),
                        recommendation: "Consider coarser partitioning strategy (e.g., partition by month instead of day). Alternatively, use Z-ordering instead of partitioning.".to_string(),
                    });
                } else if num_partitions < 10 && avg_files_per_partition > 100.0 {
                    self.insights.push(Insight {
                        severity: "info".to_string(),
                        category: "performance".to_string(),
                        title: "Under-Partitioned Table".to_string(),
                        description: format!(
                            "Table has only {} partition(s) with {:.0} files per partition on average. More granular partitioning could improve query performance.",
                            num_partitions,
                            avg_files_per_partition
                        ),
                        recommendation: "Consider finer-grained partitioning if queries frequently filter on specific columns.".to_string(),
                    });
                }
            }
        }
    }

    fn analyze_optimization_history(&mut self) {
        if self.stats.total_versions > 20 {
            if self.stats.num_files > Self::MAX_RECOMMENDED_FILES {
                self.insights.push(Insight {
                    severity: "info".to_string(),
                    category: "maintenance".to_string(),
                    title: "Consider Regular Optimization".to_string(),
                    description: format!(
                        "Table has {} versions and {} files. Regular optimization can maintain performance.",
                        self.stats.total_versions,
                        self.stats.num_files
                    ),
                    recommendation: "Set up periodic OPTIMIZE jobs (weekly or after major writes). Enable Auto Optimize for automatic compaction.".to_string(),
                });
            }
        }
    }

    fn analyze_data_skew(&mut self) {
        if self.stats.files.len() < 2 {
            return;
        }

        let file_sizes: Vec<i64> = self.stats.files.iter().map(|f| f.size_bytes).collect();
        let mean_size = file_sizes.iter().sum::<i64>() as f64 / file_sizes.len() as f64;
        let variance = file_sizes
            .iter()
            .map(|&s| {
                let diff = s as f64 - mean_size;
                diff * diff
            })
            .sum::<f64>()
            / file_sizes.len() as f64;
        let std_dev = variance.sqrt();
        let coef_variation = if mean_size > 0.0 {
            std_dev / mean_size
        } else {
            0.0
        };

        if coef_variation > Self::MIN_FILE_SIZE_VARIANCE {
            let min_size = *file_sizes.iter().min().unwrap();
            let max_size = *file_sizes.iter().max().unwrap();
            self.insights.push(Insight {
                severity: "warning".to_string(),
                category: "performance".to_string(),
                title: "Data Skew Detected".to_string(),
                description: format!(
                    "High variance in file sizes detected (CV: {:.2}). File sizes range from {} to {}. This indicates data skew which can cause uneven processing.",
                    coef_variation,
                    Self::format_bytes(min_size),
                    Self::format_bytes(max_size)
                ),
                recommendation: "Run OPTIMIZE to balance file sizes. Consider using Z-ordering or different partitioning strategy. Review data distribution in partition columns.".to_string(),
            });
        }
    }

    fn analyze_write_patterns(&mut self) {
        if self.stats.total_versions > 1 {
            let files_per_version =
                self.stats.num_files as f64 / self.stats.total_versions as f64;
            if files_per_version < 5.0 && self.stats.total_versions > 10 {
                self.insights.push(Insight {
                    severity: "info".to_string(),
                    category: "performance".to_string(),
                    title: "Many Small Writes Detected".to_string(),
                    description: format!(
                        "Table has {} versions with ~{:.1} files added per write on average. Frequent small writes create many small files.",
                        self.stats.total_versions,
                        files_per_version
                    ),
                    recommendation: "Batch writes together when possible. Enable Auto Optimize to automatically compact small files. Consider using Delta's MERGE operation for incremental updates.".to_string(),
                });
            }
        }
    }

    fn format_bytes(bytes_value: i64) -> String {
        let mut bytes = bytes_value as f64;
        let units = ["B", "KB", "MB", "GB", "TB"];
        for unit in &units {
            if bytes < 1024.0 {
                return format!("{:.2} {}", bytes, unit);
            }
            bytes /= 1024.0;
        }
        format!("{:.2} PB", bytes)
    }
}

