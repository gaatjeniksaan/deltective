"""Main CLI application for Deltective."""

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from deltective import __version__
from deltective.tui_app import run_tui

app = typer.Typer(
    name="deltective",
    help="A detective for your Delta tables - inspect, analyze, and optimize",
    add_completion=False,
)
console = Console()


def version_callback(value: bool):
    """Show version and exit."""
    if value:
        console.print(f"Deltective version: [bold cyan]{__version__}[/bold cyan]")
        raise typer.Exit()


@app.command()
def main(
    table_path: str = typer.Argument(
        ...,
        help="Path to the Delta table directory",
        metavar="TABLE_PATH",
    ),
    version: Optional[bool] = typer.Option(
        None,
        "--version",
        "-v",
        callback=version_callback,
        is_eager=True,
        help="Show version and exit",
    ),
):
    """
    Inspect a Delta Lake table with an interactive TUI.

    Launches a full-screen terminal interface with tabs for:
    - Overview: Table statistics, protocol info, and schema
    - History: Paginated view of all operations

    Examples:

        $ deltective /path/to/delta/table

        $ deltective abfss://container@account.dfs.core.windows.net/path/to/table
    """
    # Only validate local paths (not Azure storage URLs)
    if not (table_path.startswith("abfss://") or table_path.startswith("az://")):
        path = Path(table_path)
        if not path.exists():
            console.print(f"[bold red]Error:[/bold red] Path does not exist: {table_path}")
            raise typer.Exit(1)

    try:
        # Launch interactive TUI
        run_tui(table_path)

    except KeyboardInterrupt:
        # Clean exit on Ctrl+C
        console.print("\n[yellow]Exited Deltective[/yellow]")
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
