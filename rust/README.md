# Deltective (Rust)

A Rust-based implementation of Deltective - A detective for your Delta tables.

This is a full Rust port of the Python Deltective application, providing the same functionality with improved performance.

## Features

- **Interactive TUI (Text User Interface)** built with Ratatui
  - Full-screen tabbed interface with Overview, History, Insights, Configuration, and Timeline tabs
  - Switch between tabs with Tab key or arrow keys
  - Beautiful, colorful terminal output

- **Overview Tab**:
  - Table statistics (version, file count, size, rows, partitions)
  - Delta protocol information (reader/writer versions)
  - Activated table features (reader & writer features)
  - Last operation details with metrics
  - Last vacuum execution time
  - Complete schema with column types
  - Partition columns highlighted
  - Creation time and metadata

- **History Tab**:
  - Interactive paginated view of all Delta operations
  - Shows version numbers, operation types, timestamps
  - Displays operation parameters and detailed metrics
  - Shows files/rows added/removed/updated per operation

- **Insights Tab**:
  - Automated health checks and configuration analysis
  - Detects performance issues (small files, data skew, over-partitioning)
  - Cost optimization recommendations (vacuum suggestions)
  - Maintenance alerts (optimization needed, vacuum overdue)
  - Categorized by severity: Critical, Warning, Info, Good
  - Actionable recommendations for each issue

- **Configuration Tab**:
  - Table properties and custom configurations
  - Protocol versions and feature flags
  - Transaction log and checkpoint information
  - Advanced features detection:
    - Deletion Vectors, Column Mapping, Liquid Clustering
    - Timestamp NTZ, Change Data Feed, Auto Optimize
    - Data Skipping, Check Constraints, Vacuum Retention

- **Timeline Tab**:
  - Operations activity summary and trends
  - Version creation rate (versions per day)
  - Operations breakdown by type with bar charts
  - Write pattern analysis (streaming vs batch)
  - Timeline-based insights and recommendations

## Installation

```bash
cd rust
cargo build --release
```

The binary will be available at `target/release/deltective`.

## Usage

```bash
# Launch interactive TUI for a local Delta table
./target/release/deltective /path/to/delta/table

# Inspect a Delta table on Azure storage
./target/release/deltective abfss://container@account.dfs.core.windows.net/path/to/table

# Show version
./target/release/deltective --version

# Show help
./target/release/deltective --help
```

### Keyboard Controls

- `Tab` / `→` - Switch to next tab
- `←` - Switch to previous tab
- `q` - Quit application

## Development

```bash
# Build in debug mode
cargo build

# Run tests
cargo test

# Run with debug output
RUST_LOG=debug cargo run -- demo_table
```

## Dependencies

- **clap** - CLI argument parsing
- **ratatui** - Terminal UI framework
- **crossterm** - Terminal manipulation
- **deltalake** - Delta Lake table reading
- **tokio** - Async runtime
- **serde** - Serialization
- **chrono** - Date/time handling
- **anyhow** - Error handling

## Differences from Python Version

- Improved performance due to Rust's zero-cost abstractions
- Lower memory footprint
- Single binary deployment (no Python runtime required)
- Better error handling with Rust's type system

## License

Same as the main project.

