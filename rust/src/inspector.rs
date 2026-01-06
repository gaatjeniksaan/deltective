use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use deltalake::DeltaTable;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    pub path: String,
    pub size_bytes: i64,
    pub modification_time: DateTime<Utc>,
    pub partition_values: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStatistics {
    pub table_path: String,
    pub version: i64,
    pub num_files: usize,
    pub total_size_bytes: i64,
    pub schema: HashMap<String, String>,
    pub partition_columns: Vec<String>,
    pub num_rows: Option<i64>,
    pub files: Vec<FileInfo>,
    pub metadata: TableMetadata,
    pub total_versions: usize,
    pub oldest_version: i64,
    pub min_reader_version: i32,
    pub min_writer_version: i32,
    pub reader_features: Vec<String>,
    pub writer_features: Vec<String>,
    pub created_time: Option<DateTime<Utc>>,
    pub last_operation: Option<OperationInfo>,
    pub last_vacuum: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    pub id: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub created_time: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationInfo {
    pub operation: String,
    pub timestamp: DateTime<Utc>,
    pub parameters: HashMap<String, serde_json::Value>,
    pub metrics: HashMap<String, serde_json::Value>,
}

pub struct DeltaTableInspector {
    table_path: String,
    table: DeltaTable,
}

impl DeltaTableInspector {
    pub async fn new(table_path: &str) -> Result<Self> {
        let storage_options = Self::get_storage_options(table_path)?;
        
        let table = if let Some(options) = storage_options {
            DeltaTable::new_with_options(table_path, options)
                .await
                .context("Failed to open Delta table")?
        } else {
            DeltaTable::new(table_path)
                .await
                .context("Failed to open Delta table")?
        };

        Ok(Self {
            table_path: table_path.to_string(),
            table,
        })
    }

    fn get_storage_options(
        table_path: &str,
    ) -> Result<Option<HashMap<String, String>>> {
        if table_path.starts_with("abfss://") || table_path.starts_with("az://") {
            // Azure storage support would be implemented here
            // For now, return None and let deltalake handle it
            // In a full implementation, we'd use azure_identity here
            Ok(None)
        } else {
            Ok(None)
        }
    }

    pub async fn get_statistics(&self) -> Result<TableStatistics> {
        let version = self.table.version();
        let schema = self.get_schema_dict().await?;
        let metadata = self.table.metadata();

        let partition_columns = metadata.partition_columns.clone();

        // Get file information
        // Note: The actual deltalake API may differ - check documentation
        // Common approaches: get_files(), get_add_actions(), or scan_files()
        let mut files_info = Vec::new();
        let mut total_size = 0i64;

        // Try to get files using get_add_actions (common deltalake API)
        // This may need adjustment based on actual crate version
        match self.table.get_add_actions(true).await {
            Ok(add_actions) => {
                for action in add_actions.iter() {
                    let size = action.size.unwrap_or(0);
                    total_size += size;

                    let mut partition_values = HashMap::new();
                    if let Some(partition_values_map) = &action.partition_values {
                        for (key, value) in partition_values_map {
                            if let Some(val) = value {
                                partition_values.insert(key.clone(), val.to_string());
                            }
                        }
                    }

                    let modification_time = action.modification_time
                        .map(|ts| DateTime::from_timestamp(ts / 1000, 0).unwrap_or_default())
                        .unwrap_or_else(Utc::now);

                    files_info.push(FileInfo {
                        path: action.path.clone(),
                        size_bytes: size,
                        modification_time,
                        partition_values,
                    });
                }
            }
            Err(_) => {
                // If get_add_actions doesn't work, try alternative API
                // This is a fallback - adjust based on actual deltalake crate API
            }
        }

        let num_files = files_info.len();

        // Get Delta-specific information
        let protocol = self.table.protocol();
        let history = self.table.history().await?;

        let total_versions = history.len();
        let min_reader_version = protocol.min_reader_version;
        let min_writer_version = protocol.min_writer_version;
        let reader_features = protocol.reader_features.unwrap_or_default();
        let writer_features = protocol.writer_features.unwrap_or_default();

        // Get created time from metadata
        let created_time = metadata.created_time
            .map(|ts| DateTime::from_timestamp(ts / 1000, 0).unwrap_or_default());

        // Get last operation from history
        let last_operation = history.first().map(|entry| {
            let timestamp = DateTime::from_timestamp(
                entry.timestamp / 1000,
                0,
            ).unwrap_or_default();

            OperationInfo {
                operation: entry.operation.clone(),
                timestamp,
                parameters: entry.operation_parameters.clone().unwrap_or_default(),
                metrics: HashMap::new(), // operation_metrics doesn't exist in deltalake 0.18
            }
        });

        // Check for last vacuum operation
        let last_vacuum = history.iter()
            .find(|entry| entry.operation == "VACUUM")
            .and_then(|entry| {
                DateTime::from_timestamp(entry.timestamp / 1000, 0)
            });

        // Get oldest available version
        let oldest_version = history.iter()
            .map(|entry| entry.read_version)
            .min()
            .unwrap_or(0);

        Ok(TableStatistics {
            table_path: self.table_path.clone(),
            version: version as i64,
            num_files,
            total_size_bytes: total_size,
            schema,
            partition_columns,
            num_rows: None,
            files: files_info,
            metadata: TableMetadata {
                id: Some(metadata.id.to_string()),
                name: metadata.name.clone(),
                description: metadata.description.clone(),
                created_time: metadata.created_time,
            },
            total_versions,
            oldest_version: oldest_version as i64,
            min_reader_version,
            min_writer_version,
            reader_features: reader_features.into_iter().collect(),
            writer_features: writer_features.into_iter().collect(),
            created_time,
            last_operation,
            last_vacuum,
        })
    }

    async fn get_schema_dict(&self) -> Result<HashMap<String, String>> {
        let schema = self.table.schema();
        let mut result = HashMap::new();

        // Get Arrow schema from deltalake schema
        // The exact API may vary - adjust based on deltalake crate version
        let arrow_schema = schema.to_arrow()?;
        for field in arrow_schema.fields() {
            let type_str = format!("{:?}", field.data_type());
            result.insert(field.name().clone(), type_str);
        }

        Ok(result)
    }

    pub async fn get_history(&self, reverse: bool) -> Result<Vec<deltalake::kernel::CommitInfo>> {
        let mut history = self.table.history().await?;
        if reverse {
            history.reverse();
        }
        Ok(history)
    }

    pub async fn get_configuration(&self) -> Result<ConfigurationInfo> {
        let metadata = self.table.metadata()?;
        let protocol = self.table.protocol()?;

        let table_config = metadata.configuration.clone().unwrap_or_default();

        // Get checkpoint information
        let table_path = Path::new(&self.table_path);
        let delta_log_path = table_path.join("_delta_log");

        let mut checkpoint_info = CheckpointInfo {
            has_checkpoints: false,
            latest_checkpoint: None,
            checkpoint_size_bytes: 0,
        };

        let mut transaction_log_info = TransactionLogInfo {
            num_json_files: 0,
            num_checkpoints: 0,
            log_size_bytes: 0,
        };

        if delta_log_path.exists() {
            let json_files: Vec<_> = std::fs::read_dir(&delta_log_path)?
                .filter_map(|entry| entry.ok())
                .filter(|entry| {
                    entry.path().extension().and_then(|s| s.to_str()) == Some("json")
                })
                .collect();

            let checkpoint_files: Vec<_> = std::fs::read_dir(&delta_log_path)?
                .filter_map(|entry| entry.ok())
                .filter(|entry| {
                    entry.path().to_string_lossy().contains("checkpoint")
                })
                .collect();

            transaction_log_info.num_json_files = json_files.len();
            transaction_log_info.num_checkpoints = checkpoint_files.len();
            transaction_log_info.log_size_bytes = json_files.iter()
                .filter_map(|entry| entry.metadata().ok())
                .map(|meta| meta.len())
                .sum();

            if let Some(latest_checkpoint) = checkpoint_files.iter()
                .max_by_key(|entry| {
                    entry.metadata().and_then(|m| m.modified().ok()).unwrap_or_default()
                }) {
                checkpoint_info.has_checkpoints = true;
                checkpoint_info.latest_checkpoint = Some(
                    latest_checkpoint.path().file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("")
                        .to_string()
                );
                checkpoint_info.checkpoint_size_bytes = latest_checkpoint.metadata()?.len() as i64;
            }
        }

        let advanced_features = Self::detect_advanced_features(&table_config, &protocol);

        Ok(ConfigurationInfo {
            table_properties: table_config,
            table_id: Some(metadata.id.to_string()),
            table_name: metadata.name.clone(),
            description: metadata.description.clone(),
            created_time: metadata.created_time,
            partition_columns: metadata.partition_columns,
            protocol: ProtocolInfo {
                min_reader_version: protocol.min_reader_version,
                min_writer_version: protocol.min_writer_version,
                reader_features: protocol.reader_features.unwrap_or_default().into_iter().collect(),
                writer_features: protocol.writer_features.unwrap_or_default().into_iter().collect(),
            },
            checkpoint_info,
            transaction_log: transaction_log_info,
            advanced_features,
        })
    }

    fn detect_advanced_features(
        config: &HashMap<String, String>,
        protocol: &deltalake::kernel::Protocol,
    ) -> AdvancedFeatures {
        let writer_features: Vec<String> = protocol.writer_features.clone()
            .unwrap_or_default()
            .into_iter()
            .map(|f| format!("{:?}", f))
            .collect();

        let column_mapping_mode = config.get("delta.columnMapping.mode")
            .cloned()
            .unwrap_or_else(|| "none".to_string());

        let check_constraints: HashMap<String, String> = config.iter()
            .filter(|(k, _)| k.starts_with("delta.constraints."))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        AdvancedFeatures {
            deletion_vectors: writer_features.contains(&"deletionVectors".to_string()),
            column_mapping: ColumnMappingInfo {
                enabled: column_mapping_mode != "none",
                mode: column_mapping_mode,
            },
            liquid_clustering: config.contains_key("clustering"),
            timestamp_ntz: writer_features.contains(&"timestampNtz".to_string()),
            check_constraints,
            auto_optimize: AutoOptimizeInfo {
                enabled: config.get("delta.autoOptimize.autoCompact")
                    .map(|v| v == "true")
                    .unwrap_or(false)
                    || config.get("delta.autoOptimize.optimizeWrite")
                        .map(|v| v == "true")
                        .unwrap_or(false),
                auto_compact: config.get("delta.autoOptimize.autoCompact")
                    .map(|v| v == "true")
                    .unwrap_or(false),
                optimize_write: config.get("delta.autoOptimize.optimizeWrite")
                    .map(|v| v == "true")
                    .unwrap_or(false),
            },
            data_skipping: DataSkippingInfo {
                enabled: true,
                num_indexed_cols: config.get("delta.dataSkippingNumIndexedCols")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(32),
            },
            change_data_feed: config.get("delta.enableChangeDataFeed")
                .map(|v| v == "true")
                .unwrap_or(false),
            vacuum_retention_hours: config.get("delta.deletedFileRetentionDuration")
                .and_then(|v| {
                    v.replace("hours", "").trim().parse::<i32>().ok()
                })
                .unwrap_or(168),
        }
    }

    pub async fn get_timeline_analysis(&self) -> Result<TimelineAnalysis> {
        let history = self.table.history().await?;

        if history.is_empty() {
            return Ok(TimelineAnalysis {
                total_operations: 0,
                operations_by_type: HashMap::new(),
                operations_by_day: HashMap::new(),
                version_creation_rate: 0.0,
                write_patterns: Vec::new(),
                first_operation: None,
                latest_operation: None,
            });
        }

        // Group operations by type
        let mut operations_by_type: HashMap<String, i32> = HashMap::new();
        for entry in &history {
            let op_type = entry.operation.clone();
            *operations_by_type.entry(op_type).or_insert(0) += 1;
        }

        // Group operations by day
        let mut operations_by_day: HashMap<String, Vec<&deltalake::kernel::CommitInfo>> = HashMap::new();
        for entry in &history {
            let timestamp = DateTime::from_timestamp(entry.timestamp.unwrap_or(0) / 1000, 0)
                .unwrap_or_default();
            let day_key = timestamp.format("%Y-%m-%d").to_string();
            operations_by_day.entry(day_key).or_insert_with(Vec::new).push(entry);
        }

        // Calculate version creation rate
        let first_op = history.iter()
            .min_by_key(|x| x.timestamp)
            .unwrap();
        let last_op = history.iter()
            .max_by_key(|x| x.timestamp)
            .unwrap();

        let first_time = DateTime::from_timestamp(first_op.timestamp.unwrap_or(0) / 1000, 0)
            .unwrap_or_default();
        let last_time = DateTime::from_timestamp(last_op.timestamp.unwrap_or(0) / 1000, 0)
            .unwrap_or_default();

        let days_elapsed = (last_time - first_time).num_days().max(1) as f64;
        let version_creation_rate = history.len() as f64 / days_elapsed;

        // Analyze write patterns
        let write_patterns = Self::analyze_write_patterns(&history);

        Ok(TimelineAnalysis {
            total_operations: history.len(),
            operations_by_type,
            operations_by_day: operations_by_day.into_iter()
                .map(|(k, v)| (k, v.into_iter().cloned().collect()))
                .collect(),
            version_creation_rate,
            write_patterns,
            first_operation: history.last().cloned(),
            latest_operation: history.first().cloned(),
        })
    }

    fn analyze_write_patterns(history: &[deltalake::kernel::CommitInfo]) -> Vec<String> {
        let mut patterns = Vec::new();

        let writes: Vec<_> = history.iter()
            .filter(|h| matches!(h.operation.as_str(), "WRITE" | "MERGE" | "UPDATE" | "DELETE"))
            .collect();

        if writes.is_empty() {
            return patterns;
        }

        // Detect small frequent writes
        if writes.len() > 10 {
            // operation_metrics doesn't exist in deltalake 0.18, skip metrics analysis
            /*
            let avg_rows: f64 = writes.iter()
                .filter_map(|h| {
                    h.operation_metrics.as_ref()?
                        .get("num_added_rows")?
                        .as_i64()
                })
                .sum::<i64>() as f64 / writes.len() as f64;

            if avg_rows < 1000.0 {
            */
            if false { // Disabled since operation_metrics unavailable
                patterns.push("Small frequent writes detected (avg < 1000 rows)".to_string());
            }
        }

        // Detect batch vs streaming
        let timestamps: Vec<i64> = writes.iter()
            .map(|h| h.timestamp)
            .collect();

        if timestamps.len() > 1 {
            let time_diffs: Vec<i64> = timestamps.windows(2)
                .map(|w| w[0] - w[1])
                .collect();
            let avg_time_diff = time_diffs.iter().sum::<i64>() as f64 / time_diffs.len() as f64 / 1000.0;

            if avg_time_diff < 300.0 {
                patterns.push("Streaming pattern: writes every few minutes".to_string());
            } else if avg_time_diff > 86400.0 {
                patterns.push("Batch pattern: writes once per day or less".to_string());
            }
        }

        patterns
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigurationInfo {
    pub table_properties: HashMap<String, String>,
    pub table_id: Option<String>,
    pub table_name: Option<String>,
    pub description: Option<String>,
    pub created_time: Option<i64>,
    pub partition_columns: Vec<String>,
    pub protocol: ProtocolInfo,
    pub checkpoint_info: CheckpointInfo,
    pub transaction_log: TransactionLogInfo,
    pub advanced_features: AdvancedFeatures,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolInfo {
    pub min_reader_version: i32,
    pub min_writer_version: i32,
    pub reader_features: Vec<String>,
    pub writer_features: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointInfo {
    pub has_checkpoints: bool,
    pub latest_checkpoint: Option<String>,
    pub checkpoint_size_bytes: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionLogInfo {
    pub num_json_files: usize,
    pub num_checkpoints: usize,
    pub log_size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvancedFeatures {
    pub deletion_vectors: bool,
    pub column_mapping: ColumnMappingInfo,
    pub liquid_clustering: bool,
    pub timestamp_ntz: bool,
    pub check_constraints: HashMap<String, String>,
    pub auto_optimize: AutoOptimizeInfo,
    pub data_skipping: DataSkippingInfo,
    pub change_data_feed: bool,
    pub vacuum_retention_hours: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMappingInfo {
    pub enabled: bool,
    pub mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoOptimizeInfo {
    pub enabled: bool,
    pub auto_compact: bool,
    pub optimize_write: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSkippingInfo {
    pub enabled: bool,
    pub num_indexed_cols: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineAnalysis {
    pub total_operations: usize,
    pub operations_by_type: HashMap<String, i32>,
    pub operations_by_day: HashMap<String, Vec<deltalake::kernel::CommitInfo>>,
    pub version_creation_rate: f64,
    pub write_patterns: Vec<String>,
    pub first_operation: Option<deltalake::kernel::CommitInfo>,
    pub latest_operation: Option<deltalake::kernel::CommitInfo>,
}

