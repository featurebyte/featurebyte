"""
Common test fixtures used across files in integration directory
"""
import os
import sqlite3
import tempfile
from datetime import datetime

import numpy as np
import pandas as pd
import pytest
import yaml
from snowflake.connector.pandas_tools import write_pandas

from featurebyte.config import Configurations
from featurebyte.session.manager import SessionManager
from featurebyte.tile.snowflake_tile import TileSnowflake


@pytest.fixture(name="transaction_data", scope="session")
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


@pytest.fixture(name="transaction_data_upper_case", scope="session")
def transaction_dataframe_upper_case(transaction_data):
    """
    Convert transaction data column names to upper case
    """
    data = transaction_data.copy()
    data.columns = data.columns.str.upper()
    yield data


@pytest.fixture(name="sqlite_filename", scope="session")
def sqlite_filename_fixture(transaction_data):
    """
    Create SQLite database file with data for testing
    """
    with tempfile.NamedTemporaryFile() as file_handle:
        connection = sqlite3.connect(file_handle.name)
        transaction_data.to_sql(name="test_table", con=connection, index=False)
        connection.commit()
        yield file_handle.name


@pytest.fixture(name="config", scope="session")
def config_fixture(sqlite_filename):
    """
    Config object for integration testing
    """
    config_dict = {
        "datasource": [
            {
                "name": "snowflake_datasource",
                "source_type": "snowflake",
                "account": os.getenv("SNOWFLAKE_ACCOUNT"),
                "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
                "sf_schema": os.getenv("SNOWFLAKE_SCHEMA"),
                "database": os.getenv("SNOWFLAKE_DATABASE"),
                "credential_type": "USERNAME_PASSWORD",
                "username": os.getenv("SNOWFLAKE_USER"),
                "password": os.getenv("SNOWFLAKE_PASSWORD"),
            },
            {
                "name": "sqlite_datasource",
                "source_type": "sqlite",
                "filename": sqlite_filename,
            },
        ]
    }
    with tempfile.NamedTemporaryFile("w") as file_handle:
        file_handle.write(yaml.dump(config_dict))
        file_handle.flush()
        yield Configurations(config_file_path=file_handle.name)


@pytest.fixture(scope="session")
def snowflake_session(transaction_data_upper_case, config):
    """
    Snowflake session
    """
    session_manager = SessionManager(credentials=config.credentials)
    snowflake_database_source = config.db_sources["snowflake_datasource"]
    session = session_manager[snowflake_database_source]
    table_name = "TEST_TABLE"
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
    yield session


@pytest.fixture(scope="session")
def sqlite_session(config):
    """
    SQLite session
    """
    session_manager = SessionManager(credentials=config.credentials)
    sqlite_database_source = config.db_sources["sqlite_datasource"]
    return session_manager[sqlite_database_source]


@pytest.fixture(name="fb_db_session", scope="session")
def snowflake_featurebyte_session(config):
    """
    Create Snowflake session for integration tests of featurebyte sql scripts
    """
    session_manager = SessionManager(credentials=config.credentials)
    snowflake_database_source = config.db_sources["snowflake_datasource"]
    session = session_manager[snowflake_database_source]

    schema_name = os.getenv("SNOWFLAKE_SCHEMA_FEATUREBYTE")
    temp_schema_name = f"{schema_name}_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    session.execute_query(f"CREATE OR REPLACE TRANSIENT SCHEMA {temp_schema_name}")
    session.execute_query(f"USE SCHEMA {temp_schema_name}")

    sql_dir = os.path.join(os.path.dirname(__file__), "..", "..", "sql", "snowflake")
    sql_file_list = [
        "F_TIMESTAMP_TO_INDEX.sql",
        "F_INDEX_TO_TIMESTAMP.sql",
        "SP_TILE_GENERATE.sql",
        "SP_TILE_MONITOR.sql",
        "SP_TILE_GENERATE_SCHEDULE.sql",
        "SP_TILE_TRIGGER_GENERATE_SCHEDULE.sql",
        "T_TILE_REGISTRY.sql",
        "T_FEATURE_REGISTRY.sql",
    ]
    for sql_file in sql_file_list:
        with open(os.path.join(sql_dir, sql_file), encoding="utf8") as file:
            sql_script = file.read()
        session.execute_query(sql_script)

    df_tiles = pd.read_csv(os.path.join(os.path.dirname(__file__), "tile", "tile_data.csv"))
    df_tiles["TILE_START_TS"] = pd.to_datetime(df_tiles["TILE_START_TS"])
    write_pandas(
        session.connection, df_tiles, "TEMP_TABLE", auto_create_table=True, create_temp_table=True
    )

    yield session

    session.execute_query(f"DROP SCHEMA IF EXISTS {temp_schema_name}")

    session.connection.close()


@pytest.fixture
def snowflake_tile(fb_db_session, config):
    """
    Pytest Fixture for TileSnowflake instance
    """
    col_names = "TILE_START_TS,PRODUCT_ACTION,CUST_ID,VALUE"
    table_name = "TEMP_TABLE"
    tile_sql = f"SELECT {col_names} FROM {table_name} WHERE TILE_START_TS >= FB_START_TS and TILE_START_TS < FB_END_TS"
    tile_id = "tile_id1"

    tile_s = TileSnowflake(
        feature_name="feature1",
        time_modulo_frequency_seconds=183,
        blind_spot_seconds=3,
        frequency_minute=5,
        tile_sql=tile_sql,
        column_names=col_names,
        tile_id="tile_id1",
        tabular_source=config.db_sources["snowflake_datasource"],
    )

    yield tile_s

    fb_db_session.execute_query("DELETE FROM TILE_REGISTRY")
    fb_db_session.execute_query(f"DROP TABLE IF EXISTS {tile_id}")
    fb_db_session.execute_query(f"DROP TASK IF EXISTS SHELL_TASK_{tile_id}_ONLINE")
    fb_db_session.execute_query(f"DROP TASK IF EXISTS SHELL_TASK_{tile_id}_OFFLINE")
