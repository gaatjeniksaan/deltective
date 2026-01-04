"""Interactive history viewer for Delta tables."""

from datetime import datetime
from typing import List, Dict, Any

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.prompt import Prompt
from rich import box


def create_history_table(history: List[Dict[str, Any]], page: int, page_size: int) -> Table:
    """Create a table showing Delta table history for a specific page.

    Args:
        history: List of operation dictionaries
        page: Current page number (0-indexed)
        page_size: Number of items per page

    Returns:
        Rich Table with history entries for the page
    """
    start_idx = page * page_size
    end_idx = min(start_idx + page_size, len(history))
    page_history = history[start_idx:end_idx]

    table = Table(
        title=f"Delta Table History (Page {page + 1} of {(len(history) - 1) // page_size + 1})",
        box=box.ROUNDED,
        header_style="bold magenta",
        border_style="blue",
        show_lines=True,
    )

    table.add_column("Version", style="yellow", no_wrap=True, width=8)
    table.add_column("Operation", style="cyan", no_wrap=True, width=15)
    table.add_column("Timestamp", style="green", width=19)
    table.add_column("Details", style="white", no_wrap=False)

    for entry in page_history:
        version = str(entry.get("version", "?"))
        operation = entry.get("operation", "UNKNOWN")
        timestamp = datetime.fromtimestamp(entry.get("timestamp", 0) / 1000).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        # Build details string
        details = []

        # Add operation parameters
        params = entry.get("operationParameters", {})
        if params:
            param_strs = []
            for key, value in params.items():
                if key == "mode":
                    param_strs.append(f"mode={value}")
                elif key == "partitionBy":
                    param_strs.append(f"partitioned")
                elif key == "predicate":
                    param_strs.append(f"where: {value}")
            if param_strs:
                details.append(", ".join(param_strs))

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
            if metrics.get("num_updated_rows"):
                metric_strs.append(f"~{metrics['num_updated_rows']:,} rows")
            if metric_strs:
                details.append(", ".join(metric_strs))

        # Add engine info
        engine = entry.get("engineInfo", entry.get("clientVersion", ""))
        if engine:
            details.append(f"[dim]{engine}[/dim]")

        details_str = "\n".join(details) if details else "-"

        table.add_row(version, operation, timestamp, details_str)

    return table


def display_history_interactive(
    console: Console, history: List[Dict[str, Any]], page_size: int = 10
):
    """Display Delta table history with interactive pagination.

    Args:
        console: Rich console instance
        history: List of operation dictionaries
        page_size: Number of items per page (default: 10)
    """
    if not history:
        console.print("[yellow]No history available for this Delta table.[/yellow]")
        return

    current_page = 0
    total_pages = (len(history) - 1) // page_size + 1
    reverse_mode = False

    while True:
        # Clear and show header
        console.clear()
        header = Text("Delta Table History", style="bold white on blue", justify="center")
        console.print(Panel(header, border_style="bright_blue", box=box.DOUBLE))
        console.print()

        # Show sort order info
        sort_info = Text()
        sort_info.append("Sort Order: ", style="bold cyan")
        if reverse_mode:
            sort_info.append("Oldest First ↑", style="yellow")
        else:
            sort_info.append("Newest First ↓", style="green")
        console.print(Panel(sort_info, border_style="dim", box=box.ROUNDED))
        console.print()

        # Display current page
        current_history = list(reversed(history)) if reverse_mode else history
        console.print(create_history_table(current_history, current_page, page_size))
        console.print()

        # Show navigation help
        help_text = Text()
        help_text.append("Commands: ", style="bold cyan")
        help_text.append("[n]ext page", style="white")
        help_text.append(" | ", style="dim")
        help_text.append("[p]revious page", style="white")
        help_text.append(" | ", style="dim")
        help_text.append("[r]everse sort", style="white")
        help_text.append(" | ", style="dim")
        help_text.append("[q]uit", style="white")
        console.print(Panel(help_text, border_style="dim", box=box.ROUNDED))
        console.print()

        # Get user input
        command = Prompt.ask(
            "[bold cyan]Enter command[/bold cyan]",
            choices=["n", "p", "r", "q", "next", "prev", "reverse", "quit"],
            default="q",
            show_choices=False,
        ).lower()

        # Handle commands
        if command in ["q", "quit"]:
            break
        elif command in ["n", "next"]:
            if current_page < total_pages - 1:
                current_page += 1
            else:
                console.print("[yellow]Already at last page[/yellow]")
                console.input("Press Enter to continue...")
        elif command in ["p", "prev"]:
            if current_page > 0:
                current_page -= 1
            else:
                console.print("[yellow]Already at first page[/yellow]")
                console.input("Press Enter to continue...")
        elif command in ["r", "reverse"]:
            reverse_mode = not reverse_mode
            current_page = 0  # Reset to first page when reversing

    # Clear screen before exiting
    console.clear()
    console.print("[green]Exited history view[/green]")
