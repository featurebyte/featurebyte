"""
Common test fixtures used across files in integration directory
"""
import asyncio
import json
import os
import sqlite3
import tempfile
import textwrap
from datetime import datetime
from unittest import mock

import numpy as np
import pandas as pd
import pytest
import pytest_asyncio
import yaml
from bson.objectid import ObjectId
from git import Git

from featurebyte.api.entity import Entity
from featurebyte.api.event_data import EventData
from featurebyte.api.feature_store import FeatureStore
from featurebyte.config import Configurations
from featurebyte.enum import InternalName
from featurebyte.feature_manager.model import ExtendedFeatureListModel, ExtendedFeatureModel
from featurebyte.feature_manager.snowflake_feature import FeatureManagerSnowflake
from featurebyte.feature_manager.snowflake_feature_list import FeatureListManagerSnowflake
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.models.feature import FeatureModel, FeatureReadiness
from featurebyte.models.feature_list import FeatureListStatus
from featurebyte.models.feature_store import (
    DatabricksDetails,
    SnowflakeDetails,
    SQLiteDetails,
    TableDetails,
)
from featurebyte.persistent.git import GitDB
from featurebyte.session.databricks import DatabricksSession
from featurebyte.session.manager import SessionManager
from featurebyte.session.snowflake import SnowflakeSession
from featurebyte.tile.snowflake_tile import TileManagerSnowflake, TileSpec


@pytest.fixture(name="config", scope="session")
def config_fixture():
    """
    Config object for integration testing
    """
    config_dict = {
        "featurestore": [
            {
                "name": "snowflake_featurestore",
                "credential_type": "USERNAME_PASSWORD",
                "username": os.getenv("SNOWFLAKE_USER"),
                "password": os.getenv("SNOWFLAKE_PASSWORD"),
            },
            {
                "name": "sqlite_datasource",
            },
            {
                "name": "databricks_featurestore",
                "credential_type": "ACCESS_TOKEN",
                "access_token": os.getenv("DATABRICKS_ACCESS_TOKEN", ""),
            },
        ],
    }
    with tempfile.TemporaryDirectory() as temp_dir_path:
        # initialize git
        git = Git(temp_dir_path)
        git.init()

        # use local git as remote
        config_dict["git"] = {
            "remote_url": f"file://{temp_dir_path}",
            "key_path": None,
            "branch": f"integration-test-{str(ObjectId())}",
        }
        with tempfile.NamedTemporaryFile("w") as file_handle:
            file_handle.write(yaml.dump(config_dict))
            file_handle.flush()
            yield Configurations(config_file_path=file_handle.name)


@pytest.fixture(scope="session")
def event_loop():
    """
    Event loop to be used for async tests
    """
    return asyncio.get_event_loop()


@pytest.fixture(name="mock_get_persistent", scope="session")
def mock_get_persistent_fixture(config):
    """
    Mock get_persistent in featurebyte/app.py
    """
    git_db = GitDB(**config.git.dict())
    with mock.patch("featurebyte.app.get_persistent") as mock_get_persistent:
        mock_get_persistent.return_value = git_db
        yield mock_get_persistent


@pytest.fixture(name="snowflake_feature_store", scope="session")
def snowflake_feature_store_fixture(mock_get_persistent):
    """
    Snowflake feature store fixture
    """
    _ = mock_get_persistent
    schema_name = os.getenv("SNOWFLAKE_SCHEMA_FEATUREBYTE")
    temp_schema_name = f"{schema_name}_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    feature_store = FeatureStore(
        name="snowflake_featurestore",
        type="snowflake",
        details=SnowflakeDetails(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            sf_schema=temp_schema_name,
            database=os.getenv("SNOWFLAKE_DATABASE"),
        ),
    )
    feature_store.save()
    return feature_store


@pytest.fixture(name="databricks_feature_store", scope="session")
def databricks_feature_store_fixture(mock_get_persistent):
    """
    Databricks database source fixture
    """
    _ = mock_get_persistent
    schema_name = os.getenv("DATABRICKS_SCHEMA_FEATUREBYTE")
    temp_schema_name = f"{schema_name}_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    feature_store = FeatureStore(
        name="databricks_featurestore",
        type="databricks",
        details=DatabricksDetails(
            server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
            http_path=os.getenv("DATABRICKS_HTTP_PATH"),
            featurebyte_catalog=os.getenv("DATABRICKS_CATALOG"),
            featurebyte_schema=temp_schema_name,
        ),
    )
    feature_store.save()
    return feature_store


@pytest.fixture(name="sqlite_feature_store", scope="session")
def sqlite_feature_store_fixture(mock_get_persistent, sqlite_filename):
    """
    Sqlite source fixture
    """
    _ = mock_get_persistent
    return FeatureStore(
        name="sqlite_datasource",
        type="sqlite",
        details=SQLiteDetails(filename=sqlite_filename),
    )


@pytest.fixture(name="mock_config_path_env", scope="session")
def mock_config_path_env_fixture(config):
    """Override default config path for all API tests"""
    with mock.patch.dict(
        os.environ,
        {"FEATUREBYTE_CONFIG_PATH": str(config.config_file_path)},
    ):
        yield


@pytest.fixture(autouse=True, scope="session")
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
    row_number = 24 * 366
    rng = np.random.RandomState(1234)
    product_actions = ["detail", "add", "purchase", "remove", None]
    timestamps = pd.date_range("2001-01-01", freq="1h", periods=24 * 366).to_series()

    # add more points to the first one month
    first_one_month_point_num = 24 * 31
    target_point_num = 4000
    event_timestamps = np.concatenate(
        [
            rng.choice(timestamps.head(first_one_month_point_num), target_point_num),
            rng.choice(
                timestamps.tail(row_number - first_one_month_point_num),
                row_number - target_point_num,
            ),
        ]
    )
    # make timestamps unique (avoid ties when getting lags, for convenience of writing tests)
    event_timestamps += rng.rand(event_timestamps.shape[0]) * pd.Timedelta("1ms")

    data = pd.DataFrame(
        {
            "event_timestamp": event_timestamps,
            "created_at": pd.date_range("2001-01-01", freq="1min", periods=row_number),
            "cust_id": rng.randint(1, 1000, row_number),
            "user id": rng.randint(1, 10, row_number),
            "product_action": rng.choice(product_actions, row_number),
            "session_id": rng.randint(100, 1000, row_number),
        }
    )
    amount = (rng.rand(row_number) * 100).round(2)
    amount[::5] = np.nan
    data["amount"] = amount
    data["created_at"] += rng.randint(1, 100, row_number).cumsum() * pd.Timedelta(seconds=1)
    data["created_at"] = data["created_at"].astype(int)
    data["session_id"] = data["session_id"].sample(frac=1.0, random_state=0).reset_index(drop=True)

    # add some second-level variation to the event timestamp
    data["event_timestamp"] += rng.randint(0, 3600, len(timestamps)) * pd.Timedelta(seconds=1)
    # exclude any nanosecond components since it is not supported
    data["event_timestamp"] = data["event_timestamp"].dt.floor("us")
    yield data


@pytest.fixture(name="transaction_data_upper_case", scope="session")
def transaction_dataframe_upper_case(transaction_data):
    """
    Convert transaction data column names to upper case
    """
    data = transaction_data.copy()
    data.columns = data.columns.str.upper()

    # Temporary workaround of setting up test fixture for Databricks feature store due to
    # unavailability of the DBFS API. This file has to be manually uploaded via Databricks UI.
    GENERATE_CSV_OUTPUT = True
    if GENERATE_CSV_OUTPUT:
        suffix = str(datetime.today().date())
        data.to_csv(f"transactions_data_upper_case_{suffix}.csv", index=False)

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


@pytest_asyncio.fixture(name="snowflake_session", scope="session")
async def snowflake_session_fixture(transaction_data_upper_case, config, snowflake_feature_store):
    """
    Snowflake session
    """
    session_manager = SessionManager(credentials=config.credentials)
    session = await session_manager.get_session(snowflake_feature_store)
    assert isinstance(session, SnowflakeSession)

    await session.register_temp_table("TEST_TABLE", transaction_data_upper_case)

    df_tiles = pd.read_csv(os.path.join(os.path.dirname(__file__), "tile", "tile_data.csv"))
    df_tiles[InternalName.TILE_START_DATE] = pd.to_datetime(df_tiles[InternalName.TILE_START_DATE])
    await session.register_temp_table("TEMP_TABLE", df_tiles)

    yield session

    await session.execute_query(
        f"DROP SCHEMA IF EXISTS {snowflake_feature_store.details.sf_schema}"
    )


@pytest_asyncio.fixture(name="databricks_session", scope="session")
async def databricks_session_fixture(config, databricks_feature_store):
    """
    Databricks session
    """
    session_manager = SessionManager(credentials=config.credentials)
    session = await session_manager.get_session(databricks_feature_store)
    assert isinstance(session, DatabricksSession)

    await session.execute_query(
        """
        CREATE TABLE TEST_TABLE
        USING CSV
        OPTIONS (header "true", inferSchema "true")
        LOCATION 'dbfs:/FileStore/tables/transactions_data_upper_case_2022_10_17.csv';
        """
    )

    yield session

    await session.execute_query(
        f"DROP SCHEMA IF EXISTS {databricks_feature_store.details.featurebyte_schema} CASCADE"
    )


@pytest_asyncio.fixture(scope="session")
async def sqlite_session(config, sqlite_feature_store):
    """
    SQLite session
    """
    session_manager = SessionManager(credentials=config.credentials)
    return await session_manager.get_session(sqlite_feature_store)


@pytest_asyncio.fixture
async def snowflake_tile(snowflake_session):
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
        aggregation_id="agg_id1",
    )

    yield tile_spec

    await snowflake_session.execute_query("DELETE FROM TILE_REGISTRY")
    await snowflake_session.execute_query(f"DROP TABLE IF EXISTS {tile_id}")
    await snowflake_session.execute_query(f"DROP TASK IF EXISTS SHELL_TASK_{tile_id}_ONLINE")
    await snowflake_session.execute_query(f"DROP TASK IF EXISTS SHELL_TASK_{tile_id}_OFFLINE")


@pytest.fixture
def tile_manager(snowflake_session):
    """
    Feature Manager fixture
    """
    return TileManagerSnowflake(session=snowflake_session)


@pytest.fixture(name="feature_model_dict")
def feature_model_dict_feature(test_dir):
    """Fixture for a Feature dict"""
    feature_fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_sum_30m.json")
    with open(feature_fixture_path) as file_handle:
        return json.load(file_handle)


@pytest_asyncio.fixture
async def snowflake_feature(feature_model_dict, snowflake_session, snowflake_feature_store):
    """
    Fixture for a ExtendedFeatureModel object
    """
    feature_model_dict.update(
        {
            "tabular_source": {
                "feature_store_id": snowflake_feature_store.id,
                "table_details": TableDetails(table_name="some_random_table"),
            },
            "version": "v1",
            "readiness": FeatureReadiness.DRAFT,
            "is_default": True,
            "online_enabled": False,
            "event_data_ids": [
                ObjectId("626bccb9697a12204fb22ea3"),
                ObjectId("726bccb9697a12204fb22ea3"),
            ],
        }
    )
    feature = ExtendedFeatureModel(**feature_model_dict, feature_store=snowflake_feature_store)
    tile_id = feature.tile_specs[0].tile_id

    yield feature

    await snowflake_session.execute_query("DELETE FROM FEATURE_REGISTRY")
    await snowflake_session.execute_query("DELETE FROM TILE_REGISTRY")
    await snowflake_session.execute_query(f"DROP TASK IF EXISTS SHELL_TASK_{tile_id}_ONLINE")
    await snowflake_session.execute_query(f"DROP TASK IF EXISTS SHELL_TASK_{tile_id}_OFFLINE")


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
          SUM("col_float") AS value_sum_afb4d56e30a685ee9128bfa58fe4ad76d32af512
        FROM (
            SELECT
              *,
              FLOOR((DATE_PART(EPOCH_SECOND, "event_timestamp") - DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMP))) / 1800) AS tile_index
            FROM (
                SELECT
                  *
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
                )
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
        "value_column_names": ["value_sum_afb4d56e30a685ee9128bfa58fe4ad76d32af512"],
        "frequency_minute": 30,
        "tile_id": "sf_table_f1800_m300_b600_f3822df3690ac033f56672194a2f224586d0a5bd",
        "aggregation_id": "sum_afb4d56e30a685ee9128bfa58fe4ad76d32af512",
        "tile_sql": tile_sql,
        "time_modulo_frequency_second": 300,
        "category_column_name": None,
    }
    return expected_tile_spec


@pytest.fixture
def feature_manager(snowflake_session):
    """
    Feature Manager fixture
    """
    return FeatureManagerSnowflake(session=snowflake_session)


@pytest_asyncio.fixture
async def snowflake_feature_list(
    feature_model_dict, snowflake_session, config, snowflake_feature_store
):
    """
    Pytest Fixture for FeatureSnowflake instance
    """
    _ = config
    feature_model_dict.update(
        {
            "tabular_source": {
                "feature_store_id": snowflake_feature_store.id,
                "table_details": TableDetails(table_name="some_random_table"),
            },
            "version": "v1",
            "readiness": FeatureReadiness.DRAFT,
            "is_default": True,
        }
    )
    feature = FeatureModel(**feature_model_dict)

    feature_list = ExtendedFeatureListModel(
        name="feature_list1",
        feature_ids=[feature.id],
        feature_signatures=[{"id": feature.id, "name": feature.name, "version": feature.version}],
        readiness=FeatureReadiness.DRAFT,
        status=FeatureListStatus.PUBLIC_DRAFT,
        version="v1",
    )

    yield feature_list

    await snowflake_session.execute_query("DELETE FROM FEATURE_LIST_REGISTRY")
    await snowflake_session.execute_query("DELETE FROM FEATURE_REGISTRY")
    await snowflake_session.execute_query("DELETE FROM TILE_REGISTRY")


@pytest.fixture
def feature_list_manager(snowflake_session):
    """
    Feature Manager fixture
    """
    return FeatureListManagerSnowflake(session=snowflake_session)


@pytest.fixture(name="user_entity", scope="session")
def user_entity_fixture():
    """
    Fixture for an Entity "User"
    """
    entity = Entity(name="User", serving_names=["user id"])
    entity.save()
    return entity


def create_transactions_event_data_from_feature_store(
    feature_store, database_name, schema_name, table_name, event_data_name
):
    """
    Helper function to create an EventData with the given feature store
    """
    available_tables = feature_store.list_tables(
        database_name=database_name,
        schema_name=schema_name,
    )
    # check table exists (case-insensitive since some data warehouses change the casing)
    available_tables = [x.upper() for x in available_tables]
    assert table_name.upper() in available_tables

    database_table = feature_store.get_table(
        database_name=database_name,
        schema_name=schema_name,
        table_name=table_name,
    )
    expected_dtypes = pd.Series(
        {
            "EVENT_TIMESTAMP": "TIMESTAMP",
            "CREATED_AT": "INT",
            "CUST_ID": "INT",
            "USER ID": "INT",
            "PRODUCT_ACTION": "VARCHAR",
            "SESSION_ID": "INT",
            "AMOUNT": "FLOAT",
        }
    )
    pd.testing.assert_series_equal(expected_dtypes, database_table.dtypes)
    event_data = EventData.from_tabular_source(
        tabular_source=database_table,
        name=event_data_name,
        event_timestamp_column="EVENT_TIMESTAMP",
    )
    event_data.update_default_feature_job_setting(
        feature_job_setting=FeatureJobSetting(
            blind_spot="30m", frequency="1h", time_modulo_frequency="30m"
        )
    )
    event_data["USER ID"].as_entity("User")
    event_data.save()
    event_data = EventData.get(event_data_name)
    return event_data


@pytest.fixture(name="snowflake_event_data", scope="session")
def snowflake_event_data_fixture(snowflake_session, snowflake_feature_store, user_entity):
    """Fixture for an EventData in integration tests"""
    _ = user_entity
    event_data = create_transactions_event_data_from_feature_store(
        snowflake_feature_store,
        database_name=snowflake_session.database,
        schema_name=snowflake_session.sf_schema,
        table_name="TEST_TABLE",
        event_data_name="snowflake_event_data",
    )
    return event_data


@pytest.fixture(name="databricks_event_data", scope="session")
def databricks_event_data_fixture(databricks_session, databricks_feature_store, user_entity):
    """Fixture for an EventData in integration tests"""
    _ = user_entity
    event_data = create_transactions_event_data_from_feature_store(
        databricks_feature_store,
        database_name=databricks_session.featurebyte_catalog,
        schema_name=databricks_session.featurebyte_schema,
        table_name="TEST_TABLE",
        event_data_name="databricks_event_data",
    )
    return event_data


@pytest.fixture(name="event_data", scope="session")
def event_data_fixture(request):
    """
    Parametrizable fixture for EventData (possible parameters: "snowflake", "databricks")
    """
    if not hasattr(request, "param"):
        kind = "snowflake"
    else:
        kind = request.param
    assert kind in {"snowflake", "databricks"}
    if kind == "snowflake":
        return request.getfixturevalue("snowflake_event_data")
    return request.getfixturevalue("databricks_event_data")
