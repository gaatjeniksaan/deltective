#!/usr/bin/env python3
"""Script to create a demo Delta table for testing purposes."""

import pandas as pd
from deltalake import write_deltalake
from datetime import datetime, timedelta
import random

# Generate sample data
num_rows = 1000
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

print("Creating demo Delta table...")
print(f"Rows: {len(df)}")
print(f"Columns: {list(df.columns)}")
print()

# Write as Delta table with partitioning
write_deltalake(
    "demo_table",
    df,
    partition_by=["country", "department"],
    mode="overwrite",
    name="demo_users_table",
    description="Demo Delta table for testing Deltective",
)

print("Demo Delta table created successfully at: ./demo_table")
print("\nYou can now inspect it with:")
print("  deltective demo_table")
