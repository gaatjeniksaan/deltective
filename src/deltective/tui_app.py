"""Textual TUI application for Deltective."""

from datetime import datetime
from typing import List, Dict, Any

from textual.app import App, ComposeResult
from textual.containers import Container, Vertical, Horizontal, VerticalScroll
from textual.widgets import Header, Footer, TabbedContent, TabPane, Static, DataTable, Label
from textual.binding import Binding

from deltective.inspector import DeltaTableInspector, TableStatistics
from deltective.insights import DeltaTableAnalyzer, Insight


class OverviewTab(Static):
    """Widget for displaying table overview and statistics."""

    def __init__(self, stats: TableStatistics) -> None:
        super().__init__()
        self.stats = stats

    def compose(self) -> ComposeResult:
        """Compose the overview tab content."""
        yield VerticalScroll(
            Static(self._create_overview(), classes="overview-section"),
            Static(self._create_protocol_info(), classes="protocol-section"),
            Static(self._create_schema(), classes="schema-section"),
        )

    def _create_overview(self) -> str:
        """Create overview section as formatted text."""
        stats = self.stats
        lines = []
        lines.append("[bold cyan]═══ TABLE OVERVIEW ═══[/bold cyan]\n")
        lines.append(f"[cyan]Table Path:[/cyan] {stats.table_path}")
        lines.append(f"[cyan]Current Version:[/cyan] {stats.version} [dim](of {stats.total_versions} total)[/dim]")
        lines.append(f"[cyan]Oldest Available Version:[/cyan] {stats.oldest_version}")
        lines.append(f"[cyan]Number of Files:[/cyan] {stats.num_files:,}")
        lines.append(f"[cyan]Total Size:[/cyan] {self._format_bytes(stats.total_size_bytes)}")

        if stats.num_rows:
            lines.append(f"[cyan]Number of Rows:[/cyan] {stats.num_rows:,}")

        if stats.partition_columns:
            lines.append(f"[cyan]Partition Columns:[/cyan] {', '.join(stats.partition_columns)}")

        if stats.created_time:
            lines.append(f"[cyan]Created:[/cyan] {stats.created_time.strftime('%Y-%m-%d %H:%M:%S')}")

        if stats.metadata.get("name"):
            lines.append(f"[cyan]Table Name:[/cyan] {stats.metadata['name']}")

        if stats.metadata.get("description"):
            lines.append(f"[cyan]Description:[/cyan] {stats.metadata['description']}")

        return "\n".join(lines)

    def _create_protocol_info(self) -> str:
        """Create protocol information section."""
        stats = self.stats
        lines = []
        lines.append("\n[bold magenta]═══ DELTA PROTOCOL & HISTORY ═══[/bold magenta]\n")
        lines.append(f"[cyan]Min Reader Version:[/cyan] {stats.min_reader_version}")
        lines.append(f"[cyan]Min Writer Version:[/cyan] {stats.min_writer_version}")

        if stats.reader_features:
            lines.append("\n[cyan]Reader Features:[/cyan]")
            for feature in stats.reader_features:
                lines.append(f"  • {feature}")

        if stats.writer_features:
            lines.append("\n[cyan]Writer Features:[/cyan]")
            for feature in stats.writer_features:
                lines.append(f"  • {feature}")

        if stats.last_operation:
            lines.append(f"\n[cyan]Last Operation:[/cyan] {stats.last_operation['operation']}")
            lines.append(f"  [dim]Time:[/dim] {stats.last_operation['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
            if stats.last_operation.get('metrics'):
                metrics = stats.last_operation['metrics']
                if metrics.get('num_added_files'):
                    lines.append(f"  [dim]Added {metrics['num_added_files']} files, {metrics.get('num_added_rows', 0):,} rows[/dim]")

        lines.append(f"\n[cyan]Last Vacuum:[/cyan] {stats.last_vacuum.strftime('%Y-%m-%d %H:%M:%S') if stats.last_vacuum else 'Never'}")

        return "\n".join(lines)

    def _create_schema(self) -> str:
        """Create schema section."""
        stats = self.stats
        lines = []
        lines.append("\n[bold bright_green]═══ SCHEMA ═══[/bold bright_green]\n")

        for col_name, col_type in stats.schema.items():
            if col_name in stats.partition_columns:
                lines.append(f"  [yellow]{col_name}[/yellow] [dim](partition)[/dim]: [bright_green]{col_type}[/bright_green]")
            else:
                lines.append(f"  [cyan]{col_name}[/cyan]: [bright_green]{col_type}[/bright_green]")

        return "\n".join(lines)

    def _format_bytes(self, bytes_value: int) -> str:
        """Format bytes into human-readable string."""
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if bytes_value < 1024.0:
                return f"{bytes_value:.2f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.2f} PB"


class HistoryTab(Static):
    """Widget for displaying Delta table history with pagination."""

    def __init__(self, history: List[Dict[str, Any]]) -> None:
        super().__init__()
        self.history = history
        self.page = 0
        self.page_size = 10
        self.reverse = False

    def compose(self) -> ComposeResult:
        """Compose the history tab content."""
        yield Vertical(
            Label(self._get_sort_info(), id="sort-info"),
            Static("", id="history-table"),
            Label(self._get_controls_info(), id="controls-info"),
        )

    def on_mount(self) -> None:
        """Initialize the history table when mounted."""
        self.update_display()

    def _get_sort_info(self) -> str:
        """Get sort order information."""
        order = "Oldest First ↑" if self.reverse else "Newest First ↓"
        return f"[cyan]Sort Order:[/cyan] [yellow]{order}[/yellow]"

    def _get_controls_info(self) -> str:
        """Get navigation controls information."""
        total_pages = max(1, (len(self.history) - 1) // self.page_size + 1)
        current_page = self.page + 1
        return (
            f"[dim]Page {current_page} of {total_pages} | "
            f"Commands: [cyan]n[/cyan]=next, [cyan]p[/cyan]=previous, "
            f"[cyan]r[/cyan]=reverse sort[/dim]"
        )

    def update_display(self) -> None:
        """Update the history table display."""
        # Update sort info
        self.query_one("#sort-info", Label).update(self._get_sort_info())

        # Update controls info
        self.query_one("#controls-info", Label).update(self._get_controls_info())

        # Create history table
        current_history = list(reversed(self.history)) if self.reverse else self.history
        start_idx = self.page * self.page_size
        end_idx = min(start_idx + self.page_size, len(current_history))
        page_history = current_history[start_idx:end_idx]

        lines = ["\n[bold cyan]═══ OPERATION HISTORY ═══[/bold cyan]\n"]

        for entry in page_history:
            version = entry.get("version", "?")
            operation = entry.get("operation", "UNKNOWN")
            timestamp = datetime.fromtimestamp(entry.get("timestamp", 0) / 1000).strftime("%Y-%m-%d %H:%M:%S")

            lines.append(f"[yellow]Version {version}[/yellow] - [cyan]{operation}[/cyan] - [bright_green]{timestamp}[/bright_green]")

            # Add operation parameters
            params = entry.get("operationParameters", {})
            if params:
                param_strs = []
                for key, value in params.items():
                    if key == "mode":
                        param_strs.append(f"mode={value}")
                    elif key == "partitionBy":
                        param_strs.append("partitioned")
                    elif key == "predicate":
                        param_strs.append(f"where: {value}")
                if param_strs:
                    lines.append(f"  [dim]{', '.join(param_strs)}[/dim]")

            # Add metrics
            metrics = entry.get("operationMetrics", {})
            if metrics:
                metric_strs = []
                if metrics.get("num_added_files"):
                    metric_strs.append(f"+{metrics['num_added_files']} files")
                if metrics.get("num_removed_files"):
                    metric_strs.append(f"-{metrics['num_removed_files']} files")
                if metrics.get("num_added_rows"):
                    metric_strs.append(f"+{metrics['num_added_rows']:,} rows")
                if metrics.get("num_deleted_rows"):
                    metric_strs.append(f"-{metrics['num_deleted_rows']:,} rows")
                if metric_strs:
                    lines.append(f"  [dim]{', '.join(metric_strs)}[/dim]")

            # Add engine info
            engine = entry.get("engineInfo", entry.get("clientVersion", ""))
            if engine:
                lines.append(f"  [dim]{engine}[/dim]")

            lines.append("")  # Blank line between entries

        self.query_one("#history-table", Static).update("\n".join(lines))

    def next_page(self) -> None:
        """Go to next page."""
        total_pages = max(1, (len(self.history) - 1) // self.page_size + 1)
        if self.page < total_pages - 1:
            self.page += 1
            self.update_display()

    def prev_page(self) -> None:
        """Go to previous page."""
        if self.page > 0:
            self.page -= 1
            self.update_display()

    def toggle_reverse(self) -> None:
        """Toggle sort order."""
        self.reverse = not self.reverse
        self.page = 0  # Reset to first page
        self.update_display()


class InsightsTab(Static):
    """Widget for displaying table insights and recommendations."""

    def __init__(self, stats: TableStatistics) -> None:
        super().__init__()
        self.stats = stats
        self.insights: List[Insight] = []

    def compose(self) -> ComposeResult:
        """Compose the insights tab content."""
        yield VerticalScroll(
            Static(self._create_insights(), id="insights-content"),
        )

    def on_mount(self) -> None:
        """Analyze table when mounted."""
        analyzer = DeltaTableAnalyzer(self.stats)
        self.insights = analyzer.analyze()
        self.query_one("#insights-content", Static).update(self._create_insights())

    def _create_insights(self) -> str:
        """Create insights display as formatted text."""
        if not self.insights:
            return "[dim]Analyzing table...[/dim]"

        lines = []
        lines.append("[bold cyan]═══ TABLE HEALTH & RECOMMENDATIONS ═══[/bold cyan]\n")

        # Group by severity
        critical = [i for i in self.insights if i.severity == "critical"]
        warnings = [i for i in self.insights if i.severity == "warning"]
        info = [i for i in self.insights if i.severity == "info"]
        good = [i for i in self.insights if i.severity == "good"]

        # Display critical issues first
        if critical:
            lines.append("[bold red]🔴 CRITICAL ISSUES[/bold red]\n")
            for insight in critical:
                lines.append(self._format_insight(insight))
                lines.append("")

        # Display warnings
        if warnings:
            lines.append("[bold yellow]⚠️  WARNINGS[/bold yellow]\n")
            for insight in warnings:
                lines.append(self._format_insight(insight))
                lines.append("")

        # Display info/recommendations
        if info:
            lines.append("[bold bright_green]ℹ️  RECOMMENDATIONS[/bold bright_green]\n")
            for insight in info:
                lines.append(self._format_insight(insight))
                lines.append("")

        # Display positive feedback
        if good:
            lines.append("[bold bright_green]✅ GOOD CONFIGURATION[/bold bright_green]\n")
            for insight in good:
                lines.append(self._format_insight(insight))
                lines.append("")

        # Summary
        lines.append("\n[bold cyan]═══ SUMMARY ═══[/bold cyan]")
        lines.append(f"  [red]Critical:[/red] {len(critical)}")
        lines.append(f"  [yellow]Warnings:[/yellow] {len(warnings)}")
        lines.append(f"  [bright_green]Info:[/bright_green] {len(info)}")

        return "\n".join(lines)

    def _format_insight(self, insight: Insight) -> str:
        """Format a single insight for display."""
        lines = []

        # Title with emoji/icon based on severity
        if insight.severity == "critical":
            icon = "🚨"
            title_color = "bold red"
        elif insight.severity == "warning":
            icon = "⚠️"
            title_color = "bold yellow"
        elif insight.severity == "info":
            icon = "💡"
            title_color = "bold bright_green"
        else:  # good
            icon = "✓"
            title_color = "bold bright_green"

        lines.append(f"[{title_color}]{icon} {insight.title}[/{title_color}]")
        lines.append(f"[dim]Category: {insight.category.title()}[/dim]")
        lines.append(f"\n{insight.description}")
        lines.append(f"\n[cyan]→ Recommendation:[/cyan] {insight.recommendation}")

        return "\n".join(lines)


class ConfigurationTab(Static):
    """Widget for displaying table configuration and advanced features."""

    def __init__(self, table_path: str) -> None:
        super().__init__()
        self.table_path = table_path
        self.config_data = None

    def compose(self) -> ComposeResult:
        """Compose the configuration tab content."""
        yield VerticalScroll(
            Static(self._create_configuration(), id="config-content"),
        )

    def on_mount(self) -> None:
        """Load configuration when mounted."""
        inspector = DeltaTableInspector(self.table_path)
        self.config_data = inspector.get_configuration()
        self.query_one("#config-content", Static).update(self._create_configuration())

    def _create_configuration(self) -> str:
        """Create configuration display as formatted text."""
        if not self.config_data:
            return "[dim]Loading configuration...[/dim]"

        lines = []
        lines.append("[bold cyan]═══ TABLE CONFIGURATION ═══[/bold cyan]\n")

        # Table Properties
        lines.append("[bold magenta]📋 Table Properties[/bold magenta]\n")
        props = self.config_data.get("table_properties", {})
        if props:
            for key, value in sorted(props.items()):
                lines.append(f"  [cyan]{key}:[/cyan] [bright_green]{value}[/bright_green]")
        else:
            lines.append("  [dim]No custom properties configured[/dim]")

        # Table Metadata
        lines.append("\n[bold magenta]🏷️  Table Metadata[/bold magenta]\n")
        if self.config_data.get("table_id"):
            lines.append(f"  [cyan]Table ID:[/cyan] [bright_green]{self.config_data['table_id']}[/bright_green]")
        if self.config_data.get("table_name"):
            lines.append(f"  [cyan]Table Name:[/cyan] [bright_green]{self.config_data['table_name']}[/bright_green]")
        if self.config_data.get("description"):
            lines.append(f"  [cyan]Description:[/cyan] [bright_green]{self.config_data['description']}[/bright_green]")
        if self.config_data.get("partition_columns"):
            lines.append(f"  [cyan]Partition Columns:[/cyan] [bright_green]{', '.join(self.config_data['partition_columns'])}[/bright_green]")

        # Protocol Information
        lines.append("\n[bold magenta]⚙️  Protocol Versions[/bold magenta]\n")
        protocol = self.config_data.get("protocol", {})
        lines.append(f"  [cyan]Min Reader Version:[/cyan] [bright_green]{protocol.get('min_reader_version', 'N/A')}[/bright_green]")
        lines.append(f"  [cyan]Min Writer Version:[/cyan] [bright_green]{protocol.get('min_writer_version', 'N/A')}[/bright_green]")

        if protocol.get("reader_features"):
            lines.append(f"\n  [cyan]Reader Features:[/cyan]")
            for feature in protocol["reader_features"]:
                lines.append(f"    • {feature}")

        if protocol.get("writer_features"):
            lines.append(f"\n  [cyan]Writer Features:[/cyan]")
            for feature in protocol["writer_features"]:
                lines.append(f"    • {feature}")

        # Checkpoint and Transaction Log
        lines.append("\n[bold magenta]📝 Transaction Log[/bold magenta]\n")
        checkpoint = self.config_data.get("checkpoint_info", {})
        txn_log = self.config_data.get("transaction_log", {})

        if checkpoint.get("has_checkpoints"):
            lines.append(f"  [cyan]Has Checkpoints:[/cyan] [bright_green]✓ Yes[/bright_green]")
            lines.append(f"  [cyan]Latest Checkpoint:[/cyan] [bright_green]{checkpoint.get('latest_checkpoint', 'N/A')}[/bright_green]")
            lines.append(f"  [cyan]Checkpoint Size:[/cyan] [bright_green]{self._format_bytes(checkpoint.get('checkpoint_size_bytes', 0))}[/bright_green]")
        else:
            lines.append(f"  [cyan]Has Checkpoints:[/cyan] [yellow]✗ No[/yellow]")

        lines.append(f"  [cyan]JSON Files:[/cyan] [bright_green]{txn_log.get('num_json_files', 0)}[/bright_green]")
        lines.append(f"  [cyan]Log Size:[/cyan] [bright_green]{self._format_bytes(txn_log.get('log_size_bytes', 0))}[/bright_green]")

        # Advanced Features
        lines.append("\n[bold magenta]🚀 Advanced Features[/bold magenta]\n")
        features = self.config_data.get("advanced_features", {})

        # Deletion Vectors
        if features.get("deletion_vectors"):
            lines.append("  [bright_green]✓[/bright_green] [cyan]Deletion Vectors:[/cyan] [bright_green]Enabled[/bright_green]")
        else:
            lines.append("  [dim]✗ Deletion Vectors: Disabled[/dim]")

        # Column Mapping
        col_mapping = features.get("column_mapping", {})
        if col_mapping.get("enabled"):
            lines.append(f"  [bright_green]✓[/bright_green] [cyan]Column Mapping:[/cyan] [bright_green]{col_mapping.get('mode', 'unknown')}[/bright_green]")
        else:
            lines.append("  [dim]✗ Column Mapping: Disabled[/dim]")

        # Liquid Clustering
        if features.get("liquid_clustering"):
            lines.append("  [bright_green]✓[/bright_green] [cyan]Liquid Clustering:[/cyan] [bright_green]Enabled[/bright_green]")
        else:
            lines.append("  [dim]✗ Liquid Clustering: Disabled[/dim]")

        # Timestamp NTZ
        if features.get("timestamp_ntz"):
            lines.append("  [bright_green]✓[/bright_green] [cyan]Timestamp NTZ:[/cyan] [bright_green]Enabled[/bright_green]")
        else:
            lines.append("  [dim]✗ Timestamp NTZ: Disabled[/dim]")

        # Change Data Feed
        if features.get("change_data_feed"):
            lines.append("  [bright_green]✓[/bright_green] [cyan]Change Data Feed:[/cyan] [bright_green]Enabled[/bright_green]")
        else:
            lines.append("  [dim]✗ Change Data Feed: Disabled[/dim]")

        # Auto Optimize
        auto_opt = features.get("auto_optimize", {})
        if auto_opt.get("enabled"):
            opts = []
            if auto_opt.get("auto_compact"):
                opts.append("auto compact")
            if auto_opt.get("optimize_write"):
                opts.append("optimize write")
            lines.append(f"  [bright_green]✓[/bright_green] [cyan]Auto Optimize:[/cyan] [bright_green]{', '.join(opts)}[/bright_green]")
        else:
            lines.append("  [dim]✗ Auto Optimize: Disabled[/dim]")

        # Data Skipping
        data_skip = features.get("data_skipping", {})
        if data_skip.get("enabled"):
            lines.append(f"  [bright_green]✓[/bright_green] [cyan]Data Skipping:[/cyan] [bright_green]{data_skip.get('num_indexed_cols', 32)} indexed columns[/bright_green]")

        # Check Constraints
        constraints = features.get("check_constraints", {})
        if constraints:
            lines.append(f"\n  [cyan]Check Constraints:[/cyan]")
            for key, value in constraints.items():
                constraint_name = key.replace("delta.constraints.", "")
                lines.append(f"    • [bright_green]{constraint_name}:[/bright_green] {value}")

        # Vacuum Retention
        lines.append(f"\n  [cyan]Vacuum Retention:[/cyan] [bright_green]{features.get('vacuum_retention_hours', 168)} hours[/bright_green]")

        return "\n".join(lines)

    def _format_bytes(self, bytes_value: int) -> str:
        """Format bytes into human-readable string."""
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if bytes_value < 1024.0:
                return f"{bytes_value:.2f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.2f} PB"


class TimelineTab(Static):
    """Widget for displaying table timeline and operation patterns."""

    def __init__(self, table_path: str) -> None:
        super().__init__()
        self.table_path = table_path
        self.timeline_data = None

    def compose(self) -> ComposeResult:
        """Compose the timeline tab content."""
        yield VerticalScroll(
            Static(self._create_timeline(), id="timeline-content"),
        )

    def on_mount(self) -> None:
        """Load timeline data when mounted."""
        inspector = DeltaTableInspector(self.table_path)
        self.timeline_data = inspector.get_timeline_analysis()
        self.query_one("#timeline-content", Static).update(self._create_timeline())

    def _create_timeline(self) -> str:
        """Create timeline display as formatted text."""
        if not self.timeline_data:
            return "[dim]Loading timeline data...[/dim]"

        lines = []
        lines.append("[bold cyan]═══ TABLE TIMELINE & ACTIVITY ═══[/bold cyan]\n")

        # Summary Statistics
        lines.append("[bold magenta]📊 Activity Summary[/bold magenta]\n")
        total_ops = self.timeline_data.get("total_operations", 0)
        version_rate = self.timeline_data.get("version_creation_rate", 0)

        lines.append(f"  [cyan]Total Operations:[/cyan] [bright_green]{total_ops:,}[/bright_green]")
        lines.append(f"  [cyan]Version Creation Rate:[/cyan] [bright_green]{version_rate:.2f} versions/day[/bright_green]")

        # First and Latest Operations
        first_op = self.timeline_data.get("first_operation")
        latest_op = self.timeline_data.get("latest_operation")

        if first_op:
            first_time = datetime.fromtimestamp(first_op.get("timestamp", 0) / 1000)
            lines.append(f"  [cyan]First Operation:[/cyan] [bright_green]{first_time.strftime('%Y-%m-%d %H:%M:%S')}[/bright_green] [dim]({first_op.get('operation', 'N/A')})[/dim]")

        if latest_op:
            latest_time = datetime.fromtimestamp(latest_op.get("timestamp", 0) / 1000)
            lines.append(f"  [cyan]Latest Operation:[/cyan] [bright_green]{latest_time.strftime('%Y-%m-%d %H:%M:%S')}[/bright_green] [dim]({latest_op.get('operation', 'N/A')})[/dim]")

        # Operations by Type
        lines.append("\n[bold magenta]📈 Operations by Type[/bold magenta]\n")
        ops_by_type = self.timeline_data.get("operations_by_type", {})

        if ops_by_type:
            # Sort by count descending
            sorted_ops = sorted(ops_by_type.items(), key=lambda x: x[1], reverse=True)
            max_count = max(ops_by_type.values()) if ops_by_type else 1

            for op_type, count in sorted_ops:
                # Create a simple bar chart
                bar_width = int((count / max_count) * 30)  # Max 30 chars wide
                bar = "█" * bar_width
                pct = (count / total_ops * 100) if total_ops > 0 else 0
                lines.append(f"  [cyan]{op_type:15s}[/cyan] [bright_green]{bar}[/bright_green] {count:4d} [dim]({pct:.1f}%)[/dim]")
        else:
            lines.append("  [dim]No operation data available[/dim]")

        # Recent Activity (Operations by Day)
        lines.append("\n[bold magenta]📅 Recent Activity (Last 30 Days)[/bold magenta]\n")
        ops_by_day = self.timeline_data.get("operations_by_day", {})

        if ops_by_day:
            # Get last 30 days sorted
            sorted_days = sorted(ops_by_day.keys(), reverse=True)[:30]

            if sorted_days:
                lines.append(f"  {'Date':12s} {'Operations':>12s} {'Types':s}")
                lines.append(f"  {'-'*12} {'-'*12} {'-'*40}")

                for day in sorted_days:
                    day_ops = ops_by_day[day]
                    num_ops = len(day_ops)

                    # Get unique operation types for this day
                    op_types = set(op.get("operation", "UNKNOWN") for op in day_ops)
                    types_str = ", ".join(sorted(op_types)[:3])  # Show max 3 types
                    if len(op_types) > 3:
                        types_str += f" (+{len(op_types) - 3} more)"

                    lines.append(f"  [cyan]{day:12s}[/cyan] [bright_green]{num_ops:12d}[/bright_green] [dim]{types_str}[/dim]")
            else:
                lines.append("  [dim]No recent activity[/dim]")
        else:
            lines.append("  [dim]No daily activity data available[/dim]")

        # Write Patterns Analysis
        lines.append("\n[bold magenta]🔍 Write Pattern Analysis[/bold magenta]\n")
        patterns = self.timeline_data.get("write_patterns", [])

        if patterns:
            for pattern in patterns:
                lines.append(f"  • [yellow]{pattern}[/yellow]")
        else:
            lines.append("  [bright_green]✓[/bright_green] No unusual write patterns detected")

        # Recommendations based on timeline
        lines.append("\n[bold magenta]💡 Timeline Insights[/bold magenta]\n")

        if version_rate > 100:
            lines.append("  [yellow]⚠️[/yellow]  [yellow]Very high version creation rate[/yellow]")
            lines.append("     [dim]Consider running OPTIMIZE more frequently to manage file growth[/dim]")
        elif version_rate > 10:
            lines.append("  [cyan]ℹ️[/cyan]  [cyan]Moderate version creation rate[/cyan]")
            lines.append("     [dim]Regular OPTIMIZE operations recommended[/dim]")
        else:
            lines.append("  [bright_green]✓[/bright_green]  [bright_green]Normal version creation rate[/bright_green]")

        if total_ops > 100:
            lines.append(f"  [cyan]ℹ️[/cyan]  [cyan]Table has extensive history ({total_ops} operations)[/cyan]")
            lines.append("     [dim]Consider periodic VACUUM to manage storage costs[/dim]")

        return "\n".join(lines)


class DeltaInspectorApp(App):
    """Textual application for inspecting Delta tables."""

    CSS = """
    Screen {
        background: $surface;
    }

    TabbedContent {
        height: 1fr;
    }

    VerticalScroll {
        height: 1fr;
        padding: 1 2;
    }

    .overview-section, .protocol-section, .schema-section {
        padding: 1 0;
    }

    #sort-info, #controls-info {
        padding: 1 2;
        background: $boost;
    }

    #history-table {
        padding: 0 2;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit", priority=True),
        Binding("left", "prev_tab", "◀ Prev Tab", show=True),
        Binding("right", "next_tab", "Next Tab ▶", show=True),
        Binding("n", "next_page", "Next Page", show=False),
        Binding("p", "prev_page", "Prev Page", show=False),
        Binding("r", "reverse", "Reverse Sort", show=False),
    ]

    def __init__(self, table_path: str):
        super().__init__()
        self.table_path = table_path
        self.inspector = None
        self.stats = None
        self.history = None

    def compose(self) -> ComposeResult:
        """Compose the application UI."""
        yield Header(show_clock=True)
        yield Footer()

        # Load data
        self.inspector = DeltaTableInspector(self.table_path)
        self.stats = self.inspector.get_statistics()
        self.history = self.inspector.get_history()

        # Create tabs
        with TabbedContent():
            with TabPane("Overview", id="overview-tab"):
                yield OverviewTab(self.stats)
            with TabPane("History", id="history-tab"):
                yield HistoryTab(self.history)
            with TabPane("Insights", id="insights-tab"):
                yield InsightsTab(self.stats)
            with TabPane("Configuration", id="config-tab"):
                yield ConfigurationTab(self.table_path)
            with TabPane("Timeline", id="timeline-tab"):
                yield TimelineTab(self.table_path)

    def action_next_page(self) -> None:
        """Handle next page action."""
        # Check if we're on the history tab
        tabbed_content = self.query_one(TabbedContent)
        if tabbed_content.active == "history-tab":
            history_tab = self.query_one(HistoryTab)
            history_tab.next_page()

    def action_prev_page(self) -> None:
        """Handle previous page action."""
        # Check if we're on the history tab
        tabbed_content = self.query_one(TabbedContent)
        if tabbed_content.active == "history-tab":
            history_tab = self.query_one(HistoryTab)
            history_tab.prev_page()

    def action_reverse(self) -> None:
        """Handle reverse sort action."""
        # Check if we're on the history tab
        tabbed_content = self.query_one(TabbedContent)
        if tabbed_content.active == "history-tab":
            history_tab = self.query_one(HistoryTab)
            history_tab.toggle_reverse()

    def action_next_tab(self) -> None:
        """Navigate to the next tab."""
        tabbed_content = self.query_one(TabbedContent)
        tabs = ["overview-tab", "history-tab", "insights-tab", "config-tab", "timeline-tab"]
        current_index = tabs.index(tabbed_content.active) if tabbed_content.active in tabs else 0
        next_index = (current_index + 1) % len(tabs)
        tabbed_content.active = tabs[next_index]

    def action_prev_tab(self) -> None:
        """Navigate to the previous tab."""
        tabbed_content = self.query_one(TabbedContent)
        tabs = ["overview-tab", "history-tab", "insights-tab", "config-tab", "timeline-tab"]
        current_index = tabs.index(tabbed_content.active) if tabbed_content.active in tabs else 0
        prev_index = (current_index - 1) % len(tabs)
        tabbed_content.active = tabs[prev_index]


def run_tui(table_path: str) -> None:
    """Run the Textual TUI application."""
    app = DeltaInspectorApp(table_path)
    app.run()
