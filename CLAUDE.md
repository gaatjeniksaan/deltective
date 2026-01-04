# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Deltective is a Python CLI tool for inspecting Delta Lake tables with beautiful, colorful terminal output. It uses:
- **Typer** for the CLI interface
- **Rich** and **Textual** for beautiful terminal formatting (colors, panels, tables, TUI)
- **deltalake** (delta-rs Python bindings) for reading Delta tables

## Development Commands

```bash
# Setup - Create virtual environment and install
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -e ".[dev]"

# Run the CLI
deltective /path/to/delta/table

# Development tools
black src/                    # Format code
ruff check src/              # Lint code
pytest                       # Run tests (when added)

# Install just the package dependencies
pip install -e .
```

## Architecture

### Module Structure

```
src/deltective/
├── __init__.py       # Package version
├── cli.py            # Typer CLI application entry point
├── inspector.py      # Delta table inspection logic
├── insights.py       # Table health analyzer and recommendations
├── tui_app.py        # Textual TUI application with tabs
├── display.py        # Rich formatting (legacy, not used by TUI)
└── history_viewer.py # Standalone history viewer (legacy, not used by TUI)
```

### Key Components

**cli.py** - Main CLI application
- Uses Typer for command-line interface
- Single `main()` command that accepts table path
- Options: `--version/-v` (show version and exit)
- Launches the Textual TUI application via `run_tui()`
- Supports local paths and Azure storage paths (abfss://, az://)

**inspector.py** - Delta table inspection
- `DeltaTableInspector` class wraps deltalake.DeltaTable
- `get_statistics()` returns `TableStatistics` dataclass with all table info
- `get_history(reverse=False)` returns full operation history list
- Collects: version, schema, partition columns, files, metadata, protocol info, history
- `FileInfo` dataclass represents individual parquet files
- `TableStatistics` includes Delta-specific fields:
  - `total_versions`, `min_reader_version`, `min_writer_version`
  - `reader_features`, `writer_features` (activated table features)
  - `created_time`, `last_operation`, `last_vacuum`
- Azure authentication via DefaultAzureCredential (optional dependency)

**insights.py** - Table health analyzer
- `DeltaTableAnalyzer` - Analyzes Delta tables for configuration issues
- `Insight` dataclass - Represents a single insight/recommendation
- `analyze()` method runs all health checks and returns insights
- Detects issues:
  - Small files problem (< 10MB files)
  - Suboptimal average file size (far from 128MB optimal)
  - High file count (> 1000 files)
  - Data skew (high variance in file sizes)
  - Never vacuumed or vacuum overdue
  - Over/under-partitioning
  - Many small writes pattern
- Categories: performance, cost, maintenance, reliability
- Severity levels: critical, warning, info, good
- Provides actionable recommendations (OPTIMIZE, VACUUM, partitioning changes)

**tui_app.py** - Textual TUI application (main interface)
- `DeltaInspectorApp` - Main Textual app with tabbed interface
- `OverviewTab` - Widget displaying table statistics, protocol info, and schema
- `HistoryTab` - Widget displaying paginated operation history
- `InsightsTab` - Widget displaying health analysis and recommendations
- Keyboard bindings:
  - `q` - quit application
  - `left` / `right` arrow keys - navigate between tabs (prev/next)
  - `n` - next page (in History tab)
  - `p` - previous page (in History tab)
  - `r` - reverse sort order (in History tab)
  - `Tab` - switch between Overview, History, and Insights tabs (built-in)
- Uses Textual's TabbedContent for tab management
- Custom CSS for styling panels and sections
- Header shows clock, Footer shows key bindings

**display.py** - Legacy Rich formatting (not used by TUI)
- Contains functions for non-TUI display mode
- Kept for potential future non-interactive mode

**history_viewer.py** - Legacy standalone history viewer (not used by TUI)
- Contains standalone interactive history pagination
- Kept for potential future standalone usage

### Data Flow

1. CLI receives table path from user
2. DeltaTableInspector loads Delta table using deltalake library
3. Inspector collects statistics into TableStatistics dataclass
4. Display module renders statistics using Rich components
5. Output is printed to console with colors and formatting

## Important Notes

### Delta Table Reading
- Uses `deltalake` library (delta-rs Python bindings), not PySpark
- Reads Delta transaction log and metadata directly
- Does not load actual data (fast even for large tables)
- File information comes from Add actions in transaction log

### Delta Protocol Features
- Shows min reader and writer protocol versions required
- Lists activated table features (reader and writer features)
- Displays version history (total versions available)
- Shows last operation performed with timestamp and metrics
- Tracks last vacuum execution (or "Never" if not vacuumed)
- Protocol info comes from `DeltaTable.protocol()` method
- History from `DeltaTable.history()` method (list of operations)

### Display Styling
- All Rich styling uses semantic colors (cyan, green, yellow, magenta, blue)
- Panels use ROUNDED box style for softer appearance
- Tables use box.ROUNDED with show_lines for clarity
- File paths are truncated if > 60 chars to fit terminal width
- Byte sizes are formatted human-readable (B, KB, MB, GB, TB)

### CLI Entry Point
- Configured in pyproject.toml: `deltective = "deltective.cli:app"`
- After `pip install -e .`, command is available globally in venv
- Typer automatically generates help text from docstrings and type hints
