"""
Common test fixtures used across files in integration directory
"""
import os
import sqlite3
import tempfile

import numpy as np
import pandas as pd
import pytest
from snowflake.connector.pandas_tools import write_pandas

from featurebyte.session.snowflake import SnowflakeSession
from featurebyte.session.sqlite import SQLiteSession


@pytest.fixture(name="transaction_data")
def transaction_dataframe():
    """
    Simulated transaction Dataframe
    """
    # pylint: disable=E1101
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
    data["cust_id"] = data["cust_id"].sample(frac=1.0).reset_index(drop=True)
    data["session_id"] = data["session_id"].sample(frac=1.0).reset_index(drop=True)
    yield data


@pytest.fixture(name="transaction_data_upper_case")
def transaction_dataframe_upper_case(transaction_data):
    """
    Convert transaction data column names to upper case
    """
    data = transaction_data.copy()
    data.columns = data.columns.str.upper()
    yield data


@pytest.fixture()
def sqlite_session(transaction_data):
    """
    Create SQLite database file with data for testing
    """
    with tempfile.NamedTemporaryFile() as file_handle:
        connection = sqlite3.connect(file_handle.name)
        transaction_data.to_sql(name="test_table", con=connection, index=False)
        connection.commit()
        yield SQLiteSession(filename=file_handle.name)


@pytest.fixture()
def snowflake_session(transaction_data_upper_case):
    """
    Create Snowflake temporary table
    """
    database_name = os.getenv("SNOWFLAKE_DATABASE")
    schema_name = os.getenv("SNOWFLAKE_SCHEMA")
    table_name = "TEST_TABLE"
    warehouse_name = os.getenv("SNOWFLAKE_WAREHOUSE")
    session = SnowflakeSession(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=warehouse_name,
        database=database_name,
        schema=schema_name,
    )
    session.execute_query(f"USE WAREHOUSE {warehouse_name}")
    session.execute_query(
        f"""
        CREATE TEMPORARY TABLE {table_name}(
            CREATED_AT INT,
            CUST_ID INT,
            PRODUCT_ACTION STRING,
            SESSION_ID INT
        )
        """
    )
    write_pandas(session.connection, transaction_data_upper_case, table_name)
    session.database_metadata = session.populate_database_metadata()
    yield session
