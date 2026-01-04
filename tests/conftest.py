"""Pytest fixtures for Deltective tests."""

import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Generator
import random

import pandas as pd
import pytest
from deltalake import write_deltalake, DeltaTable


@pytest.fixture
def temp_delta_table(tmp_path: Path) -> Generator[Path, None, None]:
    """Create a temporary Delta table for testing.

    Creates a simple Delta table with sample data including:
    - Multiple data types (int, string, float, bool, datetime)
    - Partitioning by country and department
    - Multiple versions from append operations
    """
    table_path = tmp_path / "test_table"

    # Generate initial data
    num_rows = 100
    base_date = datetime(2024, 1, 1)

    data = {
        "id": range(1, num_rows + 1),
        "name": [f"user_{i}" for i in range(1, num_rows + 1)],
        "age": [random.randint(18, 80) for _ in range(num_rows)],
        "score": [round(random.uniform(0, 100), 2) for _ in range(num_rows)],
        "active": [random.choice([True, False]) for _ in range(num_rows)],
        "created_at": [
            base_date + timedelta(days=random.randint(0, 365))
            for _ in range(num_rows)
        ],
        "country": [
            random.choice(["US", "UK", "DE", "FR", "JP", "CA"])
            for _ in range(num_rows)
        ],
        "department": [
            random.choice(["Engineering", "Sales", "Marketing", "Support"])
            for _ in range(num_rows)
        ],
    }

    df = pd.DataFrame(data)

    # Create initial Delta table with partitioning
    write_deltalake(
        str(table_path),
        df,
        partition_by=["country", "department"],
        mode="overwrite",
        name="test_table",
        description="Test Delta table for Deltective",
    )

    # Add a few more versions via append operations
    for i in range(2, 4):
        append_data = {
            "id": range(num_rows * i, num_rows * i + 50),
            "name": [f"user_{j}" for j in range(num_rows * i, num_rows * i + 50)],
            "age": [random.randint(18, 80) for _ in range(50)],
            "score": [round(random.uniform(0, 100), 2) for _ in range(50)],
            "active": [random.choice([True, False]) for _ in range(50)],
            "created_at": [
                base_date + timedelta(days=random.randint(0, 365))
                for _ in range(50)
            ],
            "country": [
                random.choice(["US", "UK", "DE", "FR", "JP", "CA"])
                for _ in range(50)
            ],
            "department": [
                random.choice(["Engineering", "Sales", "Marketing", "Support"])
                for _ in range(50)
            ],
        }

        append_df = pd.DataFrame(append_data)
        write_deltalake(str(table_path), append_df, mode="append")

    yield table_path

    # Cleanup
    if table_path.exists():
        shutil.rmtree(table_path)


@pytest.fixture
def simple_delta_table(tmp_path: Path) -> Generator[Path, None, None]:
    """Create a simple non-partitioned Delta table for basic testing."""
    table_path = tmp_path / "simple_table"

    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "value": [10, 20, 30, 40, 50],
        "label": ["a", "b", "c", "d", "e"],
    })

    write_deltalake(
        str(table_path),
        df,
        mode="overwrite",
        name="simple_test_table",
    )

    yield table_path

    if table_path.exists():
        shutil.rmtree(table_path)


@pytest.fixture
def delta_table_with_history(tmp_path: Path) -> Generator[Path, None, None]:
    """Create a Delta table with extensive history for timeline testing."""
    table_path = tmp_path / "history_table"

    # Create initial version
    df = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
    write_deltalake(str(table_path), df, mode="overwrite")

    # Add 10 more versions with different operations
    for i in range(10):
        new_df = pd.DataFrame({
            "id": [i * 10 + j for j in range(1, 6)],
            "value": [random.randint(1, 100) for _ in range(5)]
        })
        write_deltalake(str(table_path), new_df, mode="append")

    yield table_path

    if table_path.exists():
        shutil.rmtree(table_path)


@pytest.fixture
def delta_table_with_small_files(tmp_path: Path) -> Generator[Path, None, None]:
    """Create a Delta table with many small files to test insights."""
    table_path = tmp_path / "small_files_table"

    # Create table with many small partitions (to generate small files)
    for i in range(20):
        df = pd.DataFrame({
            "id": [i],
            "value": [i * 10],
            "partition_col": [f"p{i}"],
        })

        mode = "overwrite" if i == 0 else "append"
        write_deltalake(
            str(table_path),
            df,
            partition_by=["partition_col"],
            mode=mode,
        )

    yield table_path

    if table_path.exists():
        shutil.rmtree(table_path)
