"""Tests for CLI interface."""

from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from deltective.cli import app


@pytest.fixture
def cli_runner():
    """Create a CLI runner for testing."""
    return CliRunner()


class TestCLI:
    """Test CLI commands and options."""

    def test_version_option(self, cli_runner):
        """Test --version flag."""
        result = cli_runner.invoke(app, ["--version"])

        assert result.exit_code == 0
        assert "Deltective version:" in result.stdout
        assert "0.1.0" in result.stdout

    def test_help_option(self, cli_runner):
        """Test --help flag."""
        result = cli_runner.invoke(app, ["--help"])

        assert result.exit_code == 0
        assert "deltective" in result.stdout.lower()
        assert "Delta" in result.stdout or "table" in result.stdout.lower()

    def test_missing_table_path(self, cli_runner):
        """Test error when table path is missing."""
        result = cli_runner.invoke(app, [])

        assert result.exit_code != 0

    def test_nonexistent_local_path(self, cli_runner):
        """Test error for non-existent local path."""
        result = cli_runner.invoke(app, ["/nonexistent/path"])

        assert result.exit_code == 1
        assert "Error" in result.stdout or "does not exist" in result.stdout

    def test_azure_path_not_validated(self, cli_runner):
        """Test that Azure paths are not validated locally."""
        # Azure paths should not trigger local path validation
        azure_path = "abfss://container@account.dfs.core.windows.net/table"

        # Mock run_tui to avoid actually launching the TUI
        with patch("deltective.cli.run_tui") as mock_run_tui:
            result = cli_runner.invoke(app, [azure_path])

            # Should not fail on path validation for Azure paths
            # (Will fail later when trying to connect, but that's ok for this test)
            mock_run_tui.assert_called_once_with(azure_path)

    def test_cli_with_valid_table(self, cli_runner, temp_delta_table: Path):
        """Test CLI with a valid Delta table path."""
        # Mock run_tui to avoid actually launching the TUI
        with patch("deltective.cli.run_tui") as mock_run_tui:
            result = cli_runner.invoke(app, [str(temp_delta_table)])

            mock_run_tui.assert_called_once_with(str(temp_delta_table))

    def test_cli_keyboard_interrupt(self, cli_runner, temp_delta_table: Path):
        """Test CLI handles KeyboardInterrupt gracefully."""
        with patch("deltective.cli.run_tui", side_effect=KeyboardInterrupt):
            result = cli_runner.invoke(app, [str(temp_delta_table)])

            # Should exit cleanly
            assert "Exited Deltective" in result.stdout

    def test_cli_general_exception(self, cli_runner, temp_delta_table: Path):
        """Test CLI handles general exceptions."""
        with patch("deltective.cli.run_tui", side_effect=Exception("Test error")):
            result = cli_runner.invoke(app, [str(temp_delta_table)])

            assert result.exit_code == 1
            assert "Error" in result.stdout


class TestCLIDocumentation:
    """Test CLI documentation and help text."""

    def test_command_description(self, cli_runner):
        """Test that command description is present."""
        result = cli_runner.invoke(app, ["--help"])

        assert "Delta" in result.stdout or "table" in result.stdout.lower()
        assert "inspect" in result.stdout.lower() or "TUI" in result.stdout

    def test_examples_in_help(self, cli_runner):
        """Test that help includes usage examples."""
        result = cli_runner.invoke(app, ["--help"])

        # Should show how to use the command
        assert "deltective" in result.stdout.lower() or "TABLE_PATH" in result.stdout


class TestCLIPathValidation:
    """Test path validation logic."""

    def test_local_path_validation(self, cli_runner, tmp_path: Path):
        """Test local path validation works correctly."""
        # Non-existent local path
        nonexistent = tmp_path / "nonexistent"
        result = cli_runner.invoke(app, [str(nonexistent)])

        assert result.exit_code == 1
        assert "does not exist" in result.stdout

    def test_azure_abfss_path_skips_validation(self, cli_runner):
        """Test that abfss:// paths skip local validation."""
        path = "abfss://container@account.dfs.core.windows.net/table"

        with patch("deltective.cli.run_tui") as mock_run_tui:
            result = cli_runner.invoke(app, [path])
            mock_run_tui.assert_called_once()

    def test_azure_az_path_skips_validation(self, cli_runner):
        """Test that az:// paths skip local validation."""
        path = "az://container/table"

        with patch("deltective.cli.run_tui") as mock_run_tui:
            result = cli_runner.invoke(app, [path])
            mock_run_tui.assert_called_once()
