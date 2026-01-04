#!/usr/bin/env python3
"""Script to add more history to the demo Delta table for testing."""

import pandas as pd
from deltalake import DeltaTable, write_deltalake
from datetime import datetime, timedelta
import random

# Add several append operations
for i in range(1, 6):
    print(f"Adding batch {i}...")

    # Generate new data
    num_rows = 100
    base_date = datetime(2024, 1, 1) + timedelta(days=i * 30)

    data = {
        "id": range(1000 + i * 100, 1000 + i * 100 + num_rows),
        "name": [f"user_{j}" for j in range(1000 + i * 100, 1000 + i * 100 + num_rows)],
        "age": [random.randint(18, 80) for _ in range(num_rows)],
        "score": [round(random.uniform(0, 100), 2) for _ in range(num_rows)],
        "active": [random.choice([True, False]) for _ in range(num_rows)],
        "created_at": [
            base_date + timedelta(days=random.randint(0, 30))
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

    # Append to the table
    write_deltalake(
        "demo_table",
        df,
        mode="append",
    )

print("\nDemo table history updated!")
print("You can now test the history viewer with multiple versions:")
print("  deltective demo_table")
