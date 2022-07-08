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
from featurebyte.enum import InternalName
from featurebyte.feature_manager.snowflake_feature import FeatureSnowflake
from featurebyte.models.feature import FeatureModel, FeatureReadiness, TileSpec
from featurebyte.session.manager import SessionManager
from featurebyte.session.snowflake import SnowflakeSession
from featurebyte.tile.snowflake_tile import TileSnowflake


@pytest.fixture(name="transaction_data", scope="session")
def transaction_dataframe():
    """
    Simulated transaction Dataframe
    """
    # pylint: disable=no-member
    row_number = 100
    rng = np.random.RandomState(1234)
    product_actions = ["detail", "add", "purchase", "remove", None]
    timestamps = pd.date_range("2001-01-01", freq="1h", periods=48)
    data = pd.DataFrame(
        {
            "event_timestamp": rng.choice(timestamps, row_number),
            "created_at": pd.date_range("2001-01-01", freq="1min", periods=row_number),
            "cust_id": rng.randint(1, 10, row_number),
            "user_id": rng.randint(1, 10, row_number),
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
    schema_name = os.getenv("SNOWFLAKE_SCHEMA_FEATUREBYTE")
    temp_schema_name = f"{schema_name}_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"

    config_dict = {
        "featurestore": [
            {
                "name": "snowflake_featurestore",
                "source_type": "snowflake",
                "account": os.getenv("SNOWFLAKE_ACCOUNT"),
                "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
                "database": os.getenv("SNOWFLAKE_DATABASE"),
                "sf_schema": temp_schema_name,
                "credential_type": "USERNAME_PASSWORD",
                "username": os.getenv("SNOWFLAKE_USER"),
                "password": os.getenv("SNOWFLAKE_PASSWORD"),
            },
            {
                "name": "sqlite_datasource",
                "source_type": "sqlite",
                "filename": sqlite_filename,
            },
        ],
    }
    with tempfile.NamedTemporaryFile("w") as file_handle:
        file_handle.write(yaml.dump(config_dict))
        file_handle.flush()
        yield Configurations(config_file_path=file_handle.name)


@pytest.fixture(name="snowflake_session", scope="session")
def snowflake_session_fixture(transaction_data_upper_case, config):
    """
    Snowflake session
    """
    session_manager = SessionManager(credentials=config.credentials)
    snowflake_database_source = config.feature_stores["snowflake_featurestore"]
    session = session_manager[snowflake_database_source]
    assert isinstance(session, SnowflakeSession)

    session.register_temp_table("TEST_TABLE", transaction_data_upper_case)

    df_tiles = pd.read_csv(os.path.join(os.path.dirname(__file__), "tile", "tile_data.csv"))
    df_tiles[InternalName.TILE_START_DATE] = pd.to_datetime(df_tiles[InternalName.TILE_START_DATE])
    session.register_temp_table("TEMP_TABLE", df_tiles)

    yield session

    session.execute_query(f"DROP SCHEMA IF EXISTS {snowflake_database_source.details.sf_schema}")


@pytest.fixture(scope="session")
def sqlite_session(config):
    """
    SQLite session
    """
    session_manager = SessionManager(credentials=config.credentials)
    sqlite_database_source = config.feature_stores["sqlite_datasource"]
    return session_manager[sqlite_database_source]


@pytest.fixture
def snowflake_tile(snowflake_session, config):
    """
    Pytest Fixture for TileSnowflake instance
    """
    col_names = f"{InternalName.TILE_START_DATE},PRODUCT_ACTION,CUST_ID,VALUE"
    table_name = "TEMP_TABLE"
    start = InternalName.TILE_START_DATE_SQL_PLACEHOLDER
    end = InternalName.TILE_END_DATE_SQL_PLACEHOLDER
    tile_sql = f"SELECT {col_names} FROM {table_name} WHERE {InternalName.TILE_START_DATE} >= {start} and {InternalName.TILE_START_DATE} < {end}"
    tile_id = "tile_id1"

    tile_s = TileSnowflake(
        time_modulo_frequency_seconds=183,
        blind_spot_seconds=3,
        frequency_minute=5,
        tile_sql=tile_sql,
        column_names=col_names,
        tile_id="tile_id1",
        tabular_source=config.feature_stores["snowflake_featurestore"],
        credentials=config.credentials,
    )

    yield tile_s

    snowflake_session.execute_query("DELETE FROM TILE_REGISTRY")
    snowflake_session.execute_query(f"DROP TABLE IF EXISTS {tile_id}")
    snowflake_session.execute_query(f"DROP TASK IF EXISTS SHELL_TASK_{tile_id}_ONLINE")
    snowflake_session.execute_query(f"DROP TASK IF EXISTS SHELL_TASK_{tile_id}_OFFLINE")


@pytest.fixture
def snowflake_feature(feature_model_dict, snowflake_session, config):
    """
    Pytest Fixture for FeatureSnowflake instance
    """
    mock_feature = FeatureModel(**feature_model_dict)

    tile_id = "tile_id1"
    tile_spec = TileSpec(
        tile_id=tile_id,
        time_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        tile_sql="SELECT * FROM DUMMY",
        column_names="col1",
    )

    mock_feature.tabular_source = (config.feature_stores["snowflake_featurestore"],)
    mock_feature.tile_specs = [tile_spec]
    mock_feature.version = "v1"
    mock_feature.readiness = FeatureReadiness.DRAFT
    mock_feature.is_default = True

    s_feature = FeatureSnowflake(feature=mock_feature, credentials=config.credentials)

    yield s_feature

    snowflake_session.execute_query("DELETE FROM FEATURE_REGISTRY")
    snowflake_session.execute_query("DELETE FROM TILE_REGISTRY")
    snowflake_session.execute_query(f"DROP TASK IF EXISTS SHELL_TASK_{tile_id}_ONLINE")
    snowflake_session.execute_query(f"DROP TASK IF EXISTS SHELL_TASK_{tile_id}_OFFLINE")
