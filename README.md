# Deltective

A detective for your Delta tables - inspect, analyze, and optimize Delta Lake tables with a beautiful CLI interface.

## Installation

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode
pip install -e .
```

## Usage

```bash
# Launch interactive TUI for a local Delta table
deltective /path/to/delta/table

# Inspect a Delta table on Azure storage (using default credentials)
deltective abfss://container@account.dfs.core.windows.net/path/to/table

# Show version
deltective --version

# Show help
deltective --help
```

### Interactive TUI Interface

Deltective launches a full-screen terminal interface with tabbed navigation:

**Overview Tab**
- Table statistics (version, files, size, rows)
- Delta protocol information (reader/writer versions, features)
- Table schema with partition columns highlighted
- Creation time and metadata

**History Tab**
- Paginated list of all Delta table operations (10 per page)
- Version numbers, operation types, timestamps
- Operation details (mode, partitioning, metrics)
- Sortable by newest-first or oldest-first

**Insights Tab**
- Automated health checks and configuration analysis
- Detects performance issues (small files, data skew, over-partitioning)
- Cost optimization recommendations (vacuum suggestions)
- Maintenance alerts (optimization needed, vacuum overdue)
- Categorized by severity: Critical, Warning, Info, Good
- Actionable recommendations for each issue

**Configuration Tab** (NEW!)
- Table properties and custom configurations
- Protocol versions and feature flags
- Transaction log and checkpoint information
- Advanced features detection:
  - Deletion Vectors, Column Mapping, Liquid Clustering
  - Timestamp NTZ, Change Data Feed, Auto Optimize
  - Data Skipping, Check Constraints, Vacuum Retention

**Timeline Tab** (NEW!)
- Operations activity summary and trends
- Version creation rate (versions per day)
- Operations breakdown by type with bar charts
- Recent activity by day (last 30 days)
- Write pattern analysis (streaming vs batch)
- Timeline-based insights and recommendations

**Keyboard Controls**
- `Tab` - Switch between tabs (Overview, History, Insights, Configuration, Timeline)
- `←` / `→` (Arrow keys) - Navigate between tabs (left/right)
- `n` - Next page (when in History tab)
- `p` - Previous page (when in History tab)
- `r` - Reverse sort order (when in History tab)
- `q` - Quit application

```bash
# Example: Launch TUI for demo table
deltective demo_table
```

### Azure Storage Authentication

For Azure storage paths (`abfss://`), the tool uses Azure Default Credentials, which automatically tries:
1. Environment variables (e.g., `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID`)
2. Managed Identity (when running on Azure)
3. Azure CLI credentials (`az login`)
4. Azure PowerShell credentials
5. Interactive browser authentication (if enabled)

To authenticate, simply run:
```bash
az login
```

### Quick Start with Demo Data

```bash
# Create a demo Delta table for testing
pip install pandas  # Required for demo script
python create_demo_table.py

# Launch interactive TUI for the demo table
deltective demo_table

# Then use Tab to switch between tabs
# Press 'q' to quit
```

## Features

- **Interactive TUI (Text User Interface)** built with Textual
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
  - 10 operations per page with navigation (n/p keys)
  - Toggle sort order between newest-first and oldest-first (r key)
  - Shows version numbers, operation types, timestamps
  - Displays operation parameters and detailed metrics
  - Shows files/rows added/removed/updated per operation
  - Engine/client version information

- **Insights Tab**:
  - **Automated Configuration Analysis** - Detects inefficient table settings
  - **Performance Issues**:
    - Small files problem detection
    - Data skew analysis
    - Over/under-partitioning detection
    - Suboptimal file sizes
  - **Cost Optimization**:
    - Vacuum recommendations (unused file cleanup)
    - Storage optimization suggestions
  - **Maintenance Alerts**:
    - Tables needing OPTIMIZE
    - Vacuum overdue warnings
    - Write pattern analysis
  - **Actionable Recommendations** - Specific commands and strategies
  - **Severity Levels** - Critical, Warning, Info, Good

- **Configuration Tab** (NEW!):
  - **Table Properties** - Custom Delta table configurations
  - **Table Metadata** - ID, name, description, partition columns
  - **Protocol Information** - Reader/writer versions and features
  - **Transaction Log Analysis**:
    - Checkpoint status and file information
    - JSON log file count and size
  - **Advanced Features Detection**:
    - Deletion Vectors (Delta 2.0+)
    - Column Mapping modes
    - Liquid Clustering (Delta 3.0+)
    - Timestamp without timezone support
    - Change Data Feed
    - Auto Optimize (auto compact, optimize write)
    - Data Skipping configuration
    - Check Constraints
    - Vacuum retention settings

- **Timeline Tab** (NEW!):
  - **Activity Summary** - Total operations and version creation rate
  - **Operations by Type** - Visual bar chart showing operation distribution
  - **Recent Activity** - Last 30 days of operations by day
  - **Write Pattern Analysis**:
    - Streaming vs batch pattern detection
    - Small frequent writes detection
    - Average rows per operation
  - **Timeline Insights** - Recommendations based on activity patterns
  - **First and Latest Operations** - Full lifecycle visibility

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run tests (excluding Azure tests that require Docker)
pytest tests/ --ignore=tests/test_azure_integration.py -v

# Run tests with coverage
pytest tests/ --ignore=tests/test_azure_integration.py --cov=src/deltective --cov-report=html

# Run all tests including Azure (requires Docker)
pytest tests/ -v

# Format code
black src/

# Lint code
ruff check src/
```

### Test Coverage

- **85%** coverage on core inspector logic
- **70%** coverage on insights analyzer
- **94%** coverage on CLI interface
- Integration tests with real Delta tables
- Azure storage tests with Azurite testcontainer

See [tests/README.md](tests/README.md) for detailed test documentation.
