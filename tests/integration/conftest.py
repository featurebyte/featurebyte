"""
Common test fixtures used across files in integration directory
"""
import os
import sqlite3
import tempfile
import textwrap
from datetime import datetime
from unittest import mock

import numpy as np
import pandas as pd
import pytest
import yaml
from bson.objectid import ObjectId

from featurebyte.api.entity import Entity
from featurebyte.api.event_data import EventData
from featurebyte.api.feature import Feature
from featurebyte.api.feature_store import FeatureStore
from featurebyte.config import Configurations
from featurebyte.enum import CollectionName, InternalName
from featurebyte.feature_manager.snowflake_feature import FeatureManagerSnowflake
from featurebyte.feature_manager.snowflake_feature_list import FeatureListManagerSnowflake
from featurebyte.models.feature import (
    FeatureListModel,
    FeatureListStatus,
    FeatureModel,
    FeatureReadiness,
)
from featurebyte.persistent.git import GitDB
from featurebyte.session.manager import SessionManager
from featurebyte.session.snowflake import SnowflakeSession
from featurebyte.tile.snowflake_tile import TileManagerSnowflake, TileSpec


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
        "git": {
            "remote_url": "git@github.com:featurebyte/playground.git",
            "key_path": os.getenv("GIT_SSH_KEY_PATH"),
            "branch": f"integration-test-{str(ObjectId())}",
        },
    }
    with tempfile.NamedTemporaryFile("w") as file_handle:
        file_handle.write(yaml.dump(config_dict))
        file_handle.flush()
        yield Configurations(config_file_path=file_handle.name)


@pytest.fixture(name="mock_get_persistent", scope="session")
def mock_get_persistent_fixture(config):
    """
    Mock get_persistent in featurebyte/app.py
    """
    git_db = GitDB(**config.git.dict())
    git_db.insert_doc_name_func(CollectionName.EVENT_DATA, lambda doc: doc["name"])
    with mock.patch("featurebyte.app._get_persistent") as mock_get_persistent:
        mock_get_persistent.return_value = git_db
        yield mock_get_persistent

    repo, ssh_cmd, branch = git_db.repo, git_db.ssh_cmd, git_db.branch
    origin = repo.remotes.origin
    if origin:
        with repo.git.custom_environment(GIT_SSH_COMMAND=ssh_cmd):
            origin.push(refspec=(f":{branch}"))


@pytest.fixture(name="mock_config_path_env", scope="session")
def mock_config_path_env_fixture(config):
    """Override default config path for all API tests"""
    with mock.patch.dict(
        os.environ,
        {"FEATUREBYTE_CONFIG_PATH": str(config.config_file_path)},
    ):
        yield


@pytest.fixture(autouse=True)
def mock_settings_env_vars(mock_config_path_env, mock_get_persistent):
    """Use these fixtures for all tests"""
    _ = mock_config_path_env, mock_get_persistent
    yield


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
def snowflake_tile(snowflake_session):
    """
    Pytest Fixture for TileSnowflake instance
    """
    col_names_list = [InternalName.TILE_START_DATE, "PRODUCT_ACTION", "CUST_ID", "VALUE"]
    col_names = ",".join(col_names_list)
    table_name = "TEMP_TABLE"
    start = InternalName.TILE_START_DATE_SQL_PLACEHOLDER
    end = InternalName.TILE_END_DATE_SQL_PLACEHOLDER

    tile_sql = f"SELECT {col_names} FROM {table_name} WHERE {InternalName.TILE_START_DATE} >= {start} and {InternalName.TILE_START_DATE} < {end}"
    tile_id = "tile_id1"

    tile_spec = TileSpec(
        time_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        tile_sql=tile_sql,
        column_names=col_names_list,
        entity_column_names=["PRODUCT_ACTION", "CUST_ID"],
        value_column_names=["VALUE"],
        tile_id="tile_id1",
    )

    yield tile_spec

    snowflake_session.execute_query("DELETE FROM TILE_REGISTRY")
    snowflake_session.execute_query(f"DROP TABLE IF EXISTS {tile_id}")
    snowflake_session.execute_query(f"DROP TASK IF EXISTS SHELL_TASK_{tile_id}_ONLINE")
    snowflake_session.execute_query(f"DROP TASK IF EXISTS SHELL_TASK_{tile_id}_OFFLINE")


@pytest.fixture
def tile_manager(snowflake_session):
    """
    Feature Manager fixture
    """
    return TileManagerSnowflake(session=snowflake_session)


@pytest.fixture
def snowflake_feature(feature_model_dict, snowflake_session):
    """
    Fixture for a Feature object
    """
    feature = Feature(**feature_model_dict)
    feature.version = "v1"
    feature.readiness = FeatureReadiness.DRAFT.value
    feature.is_default = True
    feature.description = "test_description_1"
    feature.online_enabled = False
    tile_id = feature.tile_specs[0].tile_id

    yield feature

    snowflake_session.execute_query("DELETE FROM FEATURE_REGISTRY")
    snowflake_session.execute_query("DELETE FROM TILE_REGISTRY")
    snowflake_session.execute_query(f"DROP TASK IF EXISTS SHELL_TASK_{tile_id}_ONLINE")
    snowflake_session.execute_query(f"DROP TASK IF EXISTS SHELL_TASK_{tile_id}_OFFLINE")


@pytest.fixture(name="snowflake_feature_expected_tile_spec_dict")
def snowflake_feature_expected_tile_spec_dict_fixture():
    """
    Fixture for the expected TileSpec dictionary corresponding to snowflake_feature above
    """
    tile_sql = textwrap.dedent(
        """
        SELECT
          TO_TIMESTAMP(DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMP)) + tile_index * 1800) AS __FB_TILE_START_DATE_COLUMN,
          "cust_id",
          SUM("col_float") AS value
        FROM (
            SELECT
              *,
              FLOOR((DATE_PART(EPOCH_SECOND, "event_timestamp") - DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMP))) / 1800) AS tile_index
            FROM (
                SELECT
                  "col_int" AS "col_int",
                  "col_float" AS "col_float",
                  "col_char" AS "col_char",
                  "col_text" AS "col_text",
                  "col_binary" AS "col_binary",
                  "col_boolean" AS "col_boolean",
                  "event_timestamp" AS "event_timestamp",
                  "created_at" AS "created_at",
                  "cust_id" AS "cust_id"
                FROM "sf_database"."sf_schema"."sf_table"
                WHERE
                  "event_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMP)
                  AND "event_timestamp" < CAST(__FB_END_DATE AS TIMESTAMP)
            )
        )
        GROUP BY
          tile_index,
          "cust_id"
        ORDER BY
          tile_index
        """
    ).strip()
    expected_tile_spec = {
        "blind_spot_second": 600,
        "entity_column_names": ["cust_id"],
        "value_column_names": ["value"],
        "frequency_minute": 30,
        "tile_id": "sum_f1800_m300_b600_3cb3b2b28a359956be02abe635c4446cb50710d7",
        "tile_sql": tile_sql,
        "time_modulo_frequency_second": 300,
    }
    return expected_tile_spec


@pytest.fixture
def feature_manager(snowflake_session):
    """
    Feature Manager fixture
    """
    return FeatureManagerSnowflake(session=snowflake_session)


@pytest.fixture
def snowflake_feature_list(feature_model_dict, snowflake_session, config):
    """
    Pytest Fixture for FeatureSnowflake instance
    """
    mock_feature = FeatureModel(**feature_model_dict)
    mock_feature.tabular_source = (config.feature_stores["snowflake_featurestore"],)
    mock_feature.version = "v1"
    mock_feature.readiness = FeatureReadiness.DRAFT.value
    mock_feature.is_default = True

    feature_list = FeatureListModel(
        name="feature_list1",
        description="test_description1",
        features=[(mock_feature.name, mock_feature.version)],
        readiness=FeatureReadiness.DRAFT,
        status=FeatureListStatus.DRAFT,
        version="v1",
    )

    yield feature_list

    snowflake_session.execute_query("DELETE FROM FEATURE_LIST_REGISTRY")
    snowflake_session.execute_query("DELETE FROM FEATURE_REGISTRY")
    snowflake_session.execute_query("DELETE FROM TILE_REGISTRY")


@pytest.fixture
def feature_list_manager(snowflake_session):
    """
    Feature Manager fixture
    """
    return FeatureListManagerSnowflake(session=snowflake_session)


@pytest.fixture(name="event_data", scope="session")
def event_data_fixture(config, snowflake_session):
    """Fixture for an EventData in integration tests"""
    table_name = "TEST_TABLE"
    snowflake_database_source = FeatureStore(
        **config.feature_stores["snowflake_featurestore"].dict()
    )
    assert table_name in snowflake_database_source.list_tables(credentials=config.credentials)

    snowflake_database_table = snowflake_database_source.get_table(
        database_name=snowflake_session.database,
        schema_name=snowflake_session.sf_schema,
        table_name=table_name,
        credentials=config.credentials,
    )
    expected_dtypes = pd.Series(
        {
            "EVENT_TIMESTAMP": "TIMESTAMP",
            "CREATED_AT": "INT",
            "CUST_ID": "INT",
            "USER_ID": "INT",
            "PRODUCT_ACTION": "VARCHAR",
            "SESSION_ID": "INT",
        }
    )
    pd.testing.assert_series_equal(expected_dtypes, snowflake_database_table.dtypes)

    # create entity & event data
    event_data = EventData.from_tabular_source(
        tabular_source=snowflake_database_table,
        name="snowflake_event_data",
        event_timestamp_column="EVENT_TIMESTAMP",
        credentials=config.credentials,
    )
    event_data.update_default_feature_job_setting(
        blind_spot="30m",
        frequency="1h",
        time_modulo_frequency="30m",
    )

    # create entity & event data
    Entity.create(name="User", serving_name="UID")
    event_data["USER_ID"].as_entity("User")
    event_data.save_as_draft()
    return event_data
