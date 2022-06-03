"""
Common test fixtures used across files in integration directory
"""
import pdb
import sqlite3
import tempfile

import numpy as np
import pandas as pd
import pytest


@pytest.fixture(name="sqlite_db_filename")
def sqlite_db_file():
    """
    Create SQLite database file with data for testing
    """
    row_number = 100
    rng = np.random.RandomState(1234)
    product_actions = ["detail", "add", "purchase", "remove", None]
    data = pd.DataFrame(
        {
            "created_at": pd.date_range("2001-01-01", freq="1min", periods=row_number),
            "cust_id": rng.randint(1, 10, row_number),
            "product_action": rng.choice(product_actions, row_number),
            "session_id": rng.randint(100, 1000, row_number),
        }
    )
    data["created_at"] += rng.randint(1, 100, row_number).cumsum() * pd.Timedelta(seconds=1)
    data["created_at"] = data["created_at"].astype(int)
    data["cust_id"] = data["cust_id"].cumsum()

    with tempfile.NamedTemporaryFile() as file_handle:
        connection = sqlite3.connect(file_handle.name)
        data.to_sql(name="browsing_raw", con=connection, index=False)
        connection.commit()
        yield file_handle.name
