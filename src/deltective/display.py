"""Rich display formatting for Deltective."""

from datetime import datetime
from typing import List, Dict, Any

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.layout import Layout
from rich.prompt import Prompt
from rich import box

from deltective.inspector import TableStatistics


def format_bytes(bytes_value: int) -> str:
    """Format bytes into human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_value < 1024.0:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.2f} PB"


def format_number(num: int) -> str:
    """Format number with comma separators."""
    return f"{num:,}"


def create_overview_panel(stats: TableStatistics) -> Panel:
    """Create an overview panel with table statistics."""
    content = Text()
    content.append("Table Path: ", style="bold cyan")
    content.append(f"{stats.table_path}\n", style="white")

    content.append("Current Version: ", style="bold cyan")
    content.append(f"{stats.version}", style="yellow")
    content.append(f" (of {stats.total_versions} total)\n", style="dim")

    content.append("Number of Files: ", style="bold cyan")
    content.append(f"{format_number(stats.num_files)}\n", style="green")

    content.append("Total Size: ", style="bold cyan")
    content.append(f"{format_bytes(stats.total_size_bytes)}\n", style="magenta")

    if stats.num_rows is not None:
        content.append("Number of Rows: ", style="bold cyan")
        content.append(f"{format_number(stats.num_rows)}\n", style="green")

    if stats.partition_columns:
        content.append("Partition Columns: ", style="bold cyan")
        content.append(f"{', '.join(stats.partition_columns)}\n", style="blue")

    # Add created time
    if stats.created_time:
        content.append("Created: ", style="bold cyan")
        content.append(f"{stats.created_time.strftime('%Y-%m-%d %H:%M:%S')}\n", style="white")

    # Add metadata if available
    if stats.metadata.get("name"):
        content.append("Table Name: ", style="bold cyan")
        content.append(f"{stats.metadata['name']}\n", style="white")

    if stats.metadata.get("description"):
        content.append("Description: ", style="bold cyan")
        content.append(f"{stats.metadata['description']}\n", style="white")

    return Panel(
        content,
        title="[bold white]Table Overview[/bold white]",
        border_style="bright_blue",
        box=box.ROUNDED,
    )


def create_delta_protocol_panel(stats: TableStatistics) -> Panel:
    """Create a panel showing Delta Lake protocol information."""
    content = Text()

    # Protocol versions
    content.append("Min Reader Version: ", style="bold cyan")
    content.append(f"{stats.min_reader_version}\n", style="green")

    content.append("Min Writer Version: ", style="bold cyan")
    content.append(f"{stats.min_writer_version}\n", style="green")

    # Reader features
    if stats.reader_features:
        content.append("\nReader Features:\n", style="bold cyan")
        for feature in stats.reader_features:
            content.append(f"  • {feature}\n", style="blue")

    # Writer features
    if stats.writer_features:
        content.append("\nWriter Features:\n", style="bold cyan")
        for feature in stats.writer_features:
            content.append(f"  • {feature}\n", style="magenta")

    # Last operation
    if stats.last_operation:
        content.append("\nLast Operation: ", style="bold cyan")
        content.append(f"{stats.last_operation['operation']}\n", style="yellow")
        content.append("  Time: ", style="bold cyan")
        content.append(
            f"{stats.last_operation['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}\n",
            style="white"
        )
        if stats.last_operation.get('metrics'):
            metrics = stats.last_operation['metrics']
            if metrics.get('num_added_files'):
                content.append(f"  Added {metrics['num_added_files']} files", style="green")
                if metrics.get('num_added_rows'):
                    content.append(f", {format_number(metrics['num_added_rows'])} rows\n", style="green")
                else:
                    content.append("\n", style="green")

    # Vacuum information
    content.append("\nLast Vacuum: ", style="bold cyan")
    if stats.last_vacuum:
        content.append(f"{stats.last_vacuum.strftime('%Y-%m-%d %H:%M:%S')}\n", style="yellow")
    else:
        content.append("Never\n", style="dim")

    return Panel(
        content,
        title="[bold white]Delta Protocol & History[/bold white]",
        border_style="bright_magenta",
        box=box.ROUNDED,
    )


def create_schema_table(stats: TableStatistics) -> Table:
    """Create a table showing the schema."""
    table = Table(
        title="Schema",
        box=box.ROUNDED,
        header_style="bold magenta",
        border_style="blue",
        show_lines=True,
    )

    table.add_column("Column Name", style="cyan", no_wrap=True)
    table.add_column("Data Type", style="green")

    for col_name, col_type in stats.schema.items():
        # Check if it's a partition column
        if col_name in stats.partition_columns:
            table.add_row(
                f"{col_name} [yellow](partition)[/yellow]",
                str(col_type),
            )
        else:
            table.add_row(col_name, str(col_type))

    return table


def create_files_table(stats: TableStatistics, limit: int = 20) -> Table:
    """Create a table showing file information."""
    table = Table(
        title=f"Files (showing {min(limit, len(stats.files))} of {len(stats.files)})",
        box=box.ROUNDED,
        header_style="bold magenta",
        border_style="blue",
        show_lines=False,
    )

    table.add_column("Path", style="cyan", no_wrap=False, max_width=60)
    table.add_column("Size", style="green", justify="right")
    table.add_column("Modified", style="yellow")

    if stats.partition_columns:
        table.add_column("Partitions", style="blue")

    # Sort files by size (largest first) and limit
    sorted_files = sorted(stats.files, key=lambda f: f.size_bytes, reverse=True)[:limit]

    for file in sorted_files:
        # Truncate path if too long
        path = file.path
        if len(path) > 60:
            path = "..." + path[-57:]

        row = [
            path,
            format_bytes(file.size_bytes),
            file.modification_time.strftime("%Y-%m-%d %H:%M:%S"),
        ]

        if stats.partition_columns:
            partition_str = ", ".join(
                f"{k}={v}" for k, v in file.partition_values.items()
            ) if file.partition_values else "-"
            row.append(partition_str)

        table.add_row(*row)

    return table


def create_file_summary_panel(stats: TableStatistics) -> Panel:
    """Create a summary panel for file statistics."""
    if not stats.files:
        content = Text("No files found", style="yellow")
    else:
        sizes = [f.size_bytes for f in stats.files]
        avg_size = sum(sizes) / len(sizes)
        min_size = min(sizes)
        max_size = max(sizes)

        content = Text()
        content.append("Average File Size: ", style="bold cyan")
        content.append(f"{format_bytes(avg_size)}\n", style="green")

        content.append("Smallest File: ", style="bold cyan")
        content.append(f"{format_bytes(min_size)}\n", style="yellow")

        content.append("Largest File: ", style="bold cyan")
        content.append(f"{format_bytes(max_size)}\n", style="red")

        # Partition distribution if applicable
        if stats.partition_columns:
            partition_count = {}
            for file in stats.files:
                for key, value in file.partition_values.items():
                    if key not in partition_count:
                        partition_count[key] = {}
                    partition_count[key][value] = partition_count[key].get(value, 0) + 1

            content.append("\nPartition Distribution:\n", style="bold cyan")
            for col, values in partition_count.items():
                content.append(f"  {col}: ", style="cyan")
                content.append(f"{len(values)} unique values\n", style="white")

    return Panel(
        content,
        title="[bold white]File Statistics[/bold white]",
        border_style="bright_green",
        box=box.ROUNDED,
    )


def display_table_info(
    console: Console,
    stats: TableStatistics,
    show_files: bool = False,
    file_limit: int = 20,
):
    """Display all table information in a beautiful format."""
    console.print()

    # Header
    header = Text("Deltective", style="bold white on blue", justify="center")
    console.print(Panel(header, border_style="bright_blue", box=box.DOUBLE))
    console.print()

    # Overview panel
    console.print(create_overview_panel(stats))
    console.print()

    # Delta protocol panel
    console.print(create_delta_protocol_panel(stats))
    console.print()

    # Schema table
    console.print(create_schema_table(stats))
    console.print()

    # File statistics
    if show_files:
        console.print(create_file_summary_panel(stats))
        console.print()
        console.print(create_files_table(stats, limit=file_limit))
        console.print()
    else:
        console.print(
            "[dim]Tip: Use --files flag to see detailed file information[/dim]"
        )
        console.print()
