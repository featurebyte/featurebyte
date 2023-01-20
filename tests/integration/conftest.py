"""
Common test fixtures used across files in integration directory
"""
# pylint: disable=too-many-lines
from typing import AsyncIterator

import asyncio
import json
import os
import sqlite3
import tempfile
import textwrap
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime
from unittest import mock

import numpy as np
import pandas as pd
import pymongo
import pytest
import pytest_asyncio
import yaml
from bson.objectid import ObjectId
from fastapi.testclient import TestClient
from mongomock_motor import AsyncMongoMockClient

from featurebyte import DatabricksDetails, DimensionView, SnowflakeDetails
from featurebyte.api.dimension_data import DimensionData
from featurebyte.api.entity import Entity
from featurebyte.api.event_data import EventData
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.item_data import ItemData
from featurebyte.api.scd_data import SlowlyChangingData
from featurebyte.app import app
from featurebyte.config import Configurations
from featurebyte.enum import InternalName
from featurebyte.feature_manager.model import ExtendedFeatureListModel, ExtendedFeatureModel
from featurebyte.feature_manager.snowflake_feature import FeatureManagerSnowflake
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.models.feature import FeatureModel, FeatureReadiness
from featurebyte.models.feature_list import FeatureListStatus
from featurebyte.persistent.mongo import MongoDB
from featurebyte.query_graph.node.schema import SQLiteDetails, TableDetails
from featurebyte.session.databricks import DatabricksSession
from featurebyte.session.manager import SessionManager
from featurebyte.session.snowflake import SnowflakeSession
from featurebyte.tile.databricks_tile import TileManagerDatabricks
from featurebyte.tile.snowflake_tile import TileManagerSnowflake, TileSpec


@pytest.fixture(name="config", scope="session")
def config_fixture():
    """
    Config object for integration testing
    """
    username_password = {
        "credential_type": "USERNAME_PASSWORD",
        "username": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
    }
    config_dict = {
        "credential": [
            {
                "feature_store": "snowflake_featurestore",
                **username_password,
            },
            {
                "feature_store": "snowflake_featurestore_invalid_because_same_schema_a",
                **username_password,
            },
            {
                "feature_store": "snowflake_featurestore_invalid_because_same_schema_b",
                **username_password,
            },
            {
                "feature_store": "snowflake_featurestore_unreachable",
                **username_password,
            },
            {
                "feature_store": "snowflake_featurestore_wrong_creds",
                "credential_type": "USERNAME_PASSWORD",
                "username": "wrong-user",
                "password": "wrong-password",
            },
            {
                "feature_store": "sqlite_datasource",
            },
            {
                "feature_store": "databricks_featurestore",
                "credential_type": "ACCESS_TOKEN",
                "access_token": os.getenv("DATABRICKS_ACCESS_TOKEN", ""),
            },
        ],
        "profile": [
            {
                "name": "local",
                "api_url": "http://localhost:8080",
                "api_token": "token",
            }
        ],
    }
    with tempfile.TemporaryDirectory() as tempdir:
        config_file_path = os.path.join(tempdir, "config.yaml")
        with open(config_file_path, "w") as file_handle:
            file_handle.write(yaml.dump(config_dict))
            file_handle.flush()
            with mock.patch("featurebyte.config.BaseAPIClient.request") as mock_request:
                with TestClient(app) as client:
                    mock_request.side_effect = client.request
                    yield Configurations(config_file_path=config_file_path)


@pytest.fixture(scope="session")
def event_loop():
    """
    Event loop to be used for async tests
    """
    return asyncio.get_event_loop()


@pytest.fixture(name="persistent", scope="session")
def mock_persistent_fixture():
    """
    Mock mongodb persistent
    """
    persistent = MongoDB(
        uri="mongodb://server.example.com:27017", database="test", client=AsyncMongoMockClient()
    )

    # skip session in unit tests
    @asynccontextmanager
    async def start_transaction() -> AsyncIterator[MongoDB]:
        yield persistent

    with mock.patch.object(persistent, "start_transaction", start_transaction):
        yield persistent


@pytest.fixture(name="mongo_persistent")
def mongo_persistent_fixture():
    """
    Mongo persistent fixture
    """
    mongo_connection = os.getenv("MONGO_CONNECTION")
    database_name = f"test_{ObjectId()}"
    client = pymongo.MongoClient(mongo_connection)
    persistent = MongoDB(uri=mongo_connection, database=database_name)
    yield persistent, client[database_name]
    client.drop_database(database_name)


@pytest.fixture(name="mock_get_persistent", scope="session")
def mock_get_persistent_fixture(persistent):
    """
    Mock get_persistent in featurebyte/app.py
    """
    with mock.patch("featurebyte.app.get_persistent") as mock_persistent:
        mock_persistent.return_value = persistent
        yield


@pytest.fixture(name="noop_validate_feature_store_id_not_used_in_warehouse")
def get_noop_validate_feature_store_id_not_used_in_warehouse_fixture():
    """
    Set a no-op validator by default.
    Functions that want to test the validation should inject an actual instance of the session validator.
    """
    with mock.patch(
        "featurebyte.service.session_validator.SessionValidatorService.validate_feature_store_id_not_used_in_warehouse"
    ) as mocked_exists:
        mocked_exists.return_value = None
        yield


@pytest.fixture(name="snowflake_details", scope="session")
def get_snowflake_details_fixture():
    """
    Get snowflake details
    """
    schema_name = os.getenv("SNOWFLAKE_SCHEMA_FEATUREBYTE")
    temp_schema_name = f"{schema_name}_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    details = SnowflakeDetails(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        sf_schema=temp_schema_name,
        database=os.getenv("SNOWFLAKE_DATABASE"),
    )
    return details


@pytest.fixture(name="snowflake_featurestore_name", scope="session")
def get_snowflake_featurestore_name_fixture():
    """
    Returns the featurestore name that is used by the default feature store in integration tests.
    """
    return "snowflake_featurestore"


@pytest.fixture(name="snowflake_feature_store", scope="session")
def snowflake_feature_store_fixture(
    mock_get_persistent, snowflake_details, snowflake_featurestore_name
):
    """
    Snowflake feature store fixture
    """
    _ = mock_get_persistent
    feature_store = FeatureStore(
        name=snowflake_featurestore_name,
        type="snowflake",
        details=snowflake_details,
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
        {"FEATUREBYTE_HOME": str(os.path.dirname(config.config_file_path))},
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
    # add timezone offset
    offsets = rng.choice(range(24), size=data["event_timestamp"].shape[0])
    data["event_timestamp"] = data["event_timestamp"] + pd.Series(offsets) * pd.Timedelta(hours=1)
    data["event_timestamp"] = pd.to_datetime(
        data["event_timestamp"].astype(str) + [f"+{i:02d}:00" for i in offsets]
    )
    data["transaction_id"] = [f"T{i}" for i in range(data.shape[0])]
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
    GENERATE_CSV_OUTPUT = False
    if GENERATE_CSV_OUTPUT:
        suffix = str(datetime.today().date())
        data.to_csv(f"transactions_data_upper_case_{suffix}.csv", index=False)

    yield data


@pytest.fixture(name="item_ids", scope="session")
def item_ids_fixture():
    """
    Fixture to get item IDs used in test data.
    """
    return [f"item_{i}" for i in range(100)]


@pytest.fixture(name="items_dataframe", scope="session")
def items_dataframe_fixture(transaction_data_upper_case, item_ids):
    """
    DataFrame fixture with item based data corresponding to the transaction data
    """
    rng = np.random.RandomState(0)  # pylint: disable=no-member
    data = defaultdict(list)
    item_types = [f"type_{i}" for i in range(100)]

    def generate_items_for_transaction(transaction_row):
        order_id = transaction_row["TRANSACTION_ID"]
        num_items = rng.randint(1, 10)
        selected_item_ids = rng.choice(item_ids, num_items, replace=False)
        selected_item_types = rng.choice(item_types, num_items, replace=False)
        data["order_id"].extend([order_id] * num_items)
        data["item_id"].extend(selected_item_ids)
        data["item_type"].extend(selected_item_types)

    for _, row in transaction_data_upper_case.iterrows():
        generate_items_for_transaction(row)

    df_items = pd.DataFrame(data)
    return df_items


@pytest.fixture(name="dimension_dataframe", scope="session")
def dimension_dataframe_fixture(item_ids):
    """
    DataFrame fixture with dimension data corresponding to items.
    """
    num_of_rows = len(item_ids)
    item_names = [f"name_{i}" for i in range(num_of_rows)]
    item_types = [f"type_{i}" for i in range(num_of_rows)]

    data = pd.DataFrame(
        {
            "created_at": pd.date_range("2001-01-01", freq="1min", periods=num_of_rows),
            "item_id": item_ids,
            "item_name": item_names,
            "item_type": item_types,
        }
    )
    yield data


@pytest.fixture(name="scd_dataframe", scope="session")
def scd_dataframe_fixture(transaction_data):
    """
    DataFrame fixture with slowly changing dimension
    """
    rng = np.random.RandomState(0)  # pylint: disable=no-member

    natural_key_values = transaction_data["user id"].unique()
    dates = pd.to_datetime(transaction_data["event_timestamp"], utc=True).dt.floor("d")
    effective_timestamp_values = pd.date_range(dates.min(), dates.max())
    # Add variations at hour level
    effective_timestamp_values += pd.to_timedelta(
        rng.randint(0, 24, len(effective_timestamp_values)), unit="h"
    )
    values = [f"STATUS_CODE_{i}" for i in range(50)]

    num_rows = 1000
    data = pd.DataFrame(
        {
            "Effective Timestamp": rng.choice(effective_timestamp_values, num_rows),
            "User ID": rng.choice(natural_key_values, num_rows),
            "User Status": rng.choice(values, num_rows),
            "ID": np.arange(num_rows),
        }
    )
    # Ensure there is only one active record per natural key as at any point in time
    data = (
        data.drop_duplicates(["User ID", "Effective Timestamp"])
        .sort_values(["User ID", "Effective Timestamp"])
        .reset_index(drop=True)
    )
    yield data


@pytest.fixture(name="expected_joined_event_item_dataframe", scope="session")
def expected_joined_event_item_dataframe_fixture(transaction_data_upper_case, items_dataframe):
    """
    DataFrame fixture with the expected joined event and item data
    """
    df = pd.merge(
        transaction_data_upper_case[
            ["TRANSACTION_ID", "EVENT_TIMESTAMP", "USER ID", "PRODUCT_ACTION"]
        ],
        items_dataframe,
        left_on="TRANSACTION_ID",
        right_on="order_id",
    )
    return df


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


@pytest.fixture(name="session_manager", scope="session")
def get_session_manager(config):
    """
    Fixture to return a session manager with real login credentials
    """
    return SessionManager(credentials=config.credentials)


@pytest.fixture(name="dimension_data_table_name", scope="session")
def dimension_data_table_name_fixture():
    """
    Get the dimension data table name used in integration tests.
    """
    return "DIMENSION_DATA_TABLE"


@pytest.fixture(name="scd_data_table_name", scope="session")
def scd_data_table_name_fixture():
    """
    Get the scd data table name used in integration tests.
    """
    return "SCD_DATA_TABLE"


@pytest_asyncio.fixture(name="snowflake_session", scope="session")
async def snowflake_session_fixture(
    transaction_data_upper_case,
    items_dataframe,
    dimension_dataframe,
    dimension_data_table_name,
    scd_dataframe,
    scd_data_table_name,
    session_manager,
    snowflake_feature_store,
):
    """
    Snowflake session
    """
    session = await session_manager.get_session(snowflake_feature_store)
    assert isinstance(session, SnowflakeSession)

    # EventData table
    await session.register_table("TEST_TABLE", transaction_data_upper_case, temporary=False)

    # ItemData table
    await session.register_table("ITEM_DATA_TABLE", items_dataframe, temporary=False)

    # DimensionData table
    await session.register_table(dimension_data_table_name, dimension_dataframe, temporary=False)

    # SCD table
    await session.register_table(scd_data_table_name, scd_dataframe, temporary=False)

    # Tile table for tile manager integration tests
    df_tiles = pd.read_csv(os.path.join(os.path.dirname(__file__), "tile", "tile_data.csv"))
    # Tile table timestamps are TZ-naive
    df_tiles[InternalName.TILE_START_DATE] = pd.to_datetime(
        df_tiles[InternalName.TILE_START_DATE], utc=True
    ).dt.tz_localize(None)
    await session.register_table("TEMP_TABLE", df_tiles, temporary=False)

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
    tile_id = "TILE_ID1"

    tile_spec = TileSpec(
        time_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        tile_sql=tile_sql,
        column_names=col_names_list,
        entity_column_names=["PRODUCT_ACTION", "CUST_ID"],
        value_column_names=["VALUE"],
        value_column_types=["FLOAT"],
        tile_id=tile_id,
        aggregation_id="agg_id1",
    )

    yield tile_spec

    await snowflake_session.execute_query("DELETE FROM TILE_REGISTRY")
    await snowflake_session.execute_query(
        f"DROP TABLE IF EXISTS {tile_spec.aggregation_id}_ENTITY_TRACKER"
    )
    await snowflake_session.execute_query(f"DROP TABLE IF EXISTS {tile_id}")
    await snowflake_session.execute_query(f"DROP TASK IF EXISTS SHELL_TASK_{tile_id}_ONLINE")
    await snowflake_session.execute_query(f"DROP TASK IF EXISTS SHELL_TASK_{tile_id}_OFFLINE")


@pytest_asyncio.fixture
async def databricks_tile_spec(databricks_session):
    """
    Pytest Fixture for TileSnowflake instance
    """
    col_names_list = [InternalName.TILE_START_DATE, "PRODUCT_ACTION", "CUST_ID", "VALUE"]
    col_names = ",".join(col_names_list)
    table_name = "default.TEST_TILE_TABLE_2"
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

    await databricks_session.execute_query(f"DROP TABLE IF EXISTS {tile_id}")


@pytest.fixture
def tile_manager(snowflake_session):
    """
    Feature Manager fixture
    """
    return TileManagerSnowflake(session=snowflake_session)


@pytest.fixture
def tile_manager_databricks(databricks_session):
    """
    Feature Manager fixture
    """
    return TileManagerDatabricks(session=databricks_session)


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
            "online_enabled": False,
            "tabular_data_ids": [
                ObjectId("626bccb9697a12204fb22ea3"),
                ObjectId("726bccb9697a12204fb22ea3"),
            ],
        }
    )
    feature = ExtendedFeatureModel(**feature_model_dict)
    tile_id = feature.tile_specs[0].tile_id

    yield feature

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

    await snowflake_session.execute_query("DELETE FROM TILE_REGISTRY")


@pytest.fixture(name="user_entity", scope="session")
def user_entity_fixture():
    """
    Fixture for an Entity "User"
    """
    entity = Entity(name="User", serving_names=["user id"])
    entity.save()
    return entity


@pytest.fixture(name="product_action_entity", scope="session")
def product_action_entity_fixture():
    """
    Fixture for an Entity "ProductAction"
    """
    entity = Entity(name="ProductAction", serving_names=["PRODUCT_ACTION"])
    entity.save()
    return entity


@pytest.fixture(name="customer_entity", scope="session")
def customer_entity_fixture():
    """
    Fixture for an Entity "Customer"
    """
    entity = Entity(name="Customer", serving_names=["cust_id"])
    entity.save()
    return entity


@pytest.fixture(name="order_entity", scope="session")
def order_entity_fixture():
    """
    Fixture for an Entity "Order"
    """
    entity = Entity(name="Order", serving_names=["order_id"])
    entity.save()
    return entity


@pytest.fixture(name="item_entity", scope="session")
def item_entity_fixture():
    """
    Fixture for an Entity "Item"
    """
    entity = Entity(name="Item", serving_names=["item_id"])
    entity.save()
    return entity


@pytest.fixture(name="status_entity", scope="session")
def status_entity_fixture():
    """
    Fixture for an Entity "UserStatus"
    """
    entity = Entity(name="UserStatus", serving_names=["user_status"])
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
            "EVENT_TIMESTAMP": "TIMESTAMP_TZ",
            "CREATED_AT": "INT",
            "CUST_ID": "INT",
            "USER ID": "INT",
            "PRODUCT_ACTION": "VARCHAR",
            "SESSION_ID": "INT",
            "AMOUNT": "FLOAT",
            "TRANSACTION_ID": "VARCHAR",
        }
    )
    pd.testing.assert_series_equal(expected_dtypes, database_table.dtypes)
    event_data = EventData.from_tabular_source(
        tabular_source=database_table,
        name=event_data_name,
        event_id_column="TRANSACTION_ID",
        event_timestamp_column="EVENT_TIMESTAMP",
    )
    event_data.update_default_feature_job_setting(
        feature_job_setting=FeatureJobSetting(
            blind_spot="30m", frequency="1h", time_modulo_frequency="30m"
        )
    )
    event_data["USER ID"].as_entity("User")
    event_data["PRODUCT_ACTION"].as_entity("ProductAction")
    event_data["CUST_ID"].as_entity("Customer")
    event_data.save()
    event_data = EventData.get(event_data_name)
    return event_data


@pytest.fixture(name="snowflake_event_data", scope="session")
def snowflake_event_data_fixture(
    snowflake_session,
    snowflake_feature_store,
    user_entity,
    product_action_entity,
    customer_entity,
):
    """Fixture for an EventData in integration tests"""
    _ = user_entity
    _ = product_action_entity
    _ = customer_entity
    event_data = create_transactions_event_data_from_feature_store(
        snowflake_feature_store,
        database_name=snowflake_session.database,
        schema_name=snowflake_session.sf_schema,
        table_name="TEST_TABLE",
        event_data_name="snowflake_event_data",
    )
    return event_data


@pytest.fixture(name="snowflake_item_data", scope="session")
def snowflake_item_data_fixture(
    snowflake_session,
    snowflake_feature_store,
    snowflake_event_data,
    order_entity,
    item_entity,
):
    """Fixture for an ItemData in integration tests"""
    database_table = snowflake_feature_store.get_table(
        database_name=snowflake_session.database_name,
        schema_name=snowflake_session.sf_schema,
        table_name="ITEM_DATA_TABLE",
    )
    item_data_name = "snowflake_item_data"
    item_data = ItemData.from_tabular_source(
        tabular_source=database_table,
        name=item_data_name,
        event_id_column="order_id",
        item_id_column="item_id",
        event_data_name=snowflake_event_data.name,
    )
    item_data.save()
    item_data = ItemData.get(item_data_name)
    item_data["order_id"].as_entity(order_entity.name)
    item_data["item_id"].as_entity(item_entity.name)
    return item_data


@pytest.fixture(name="snowflake_dimension_data", scope="session")
def snowflake_dimension_data_fixture(
    snowflake_session,
    snowflake_feature_store,
    dimension_data_table_name,
    item_entity,
):
    """
    Fixture for a DimensionData in integration tests
    """
    database_table = snowflake_feature_store.get_table(
        database_name=snowflake_session.database_name,
        schema_name=snowflake_session.sf_schema,
        table_name=dimension_data_table_name,
    )
    dimension_data_name = "snowflake_dimension_data"
    dimension_data = DimensionData.from_tabular_source(
        tabular_source=database_table,
        name=dimension_data_name,
        dimension_id_column="item_id",
    )
    dimension_data.save()
    dimension_data = DimensionData.get(dimension_data_name)
    dimension_data["item_id"].as_entity(item_entity.name)
    return dimension_data


@pytest.fixture(name="scd_data_tabular_source", scope="session")
def get_scd_data_tabular_source_fixture(
    snowflake_session,
    snowflake_feature_store,
    scd_data_table_name,
):
    """
    Fixture for scd data tabular source
    """
    database_table = snowflake_feature_store.get_table(
        database_name=snowflake_session.database_name,
        schema_name=snowflake_session.sf_schema,
        table_name=scd_data_table_name,
    )
    return database_table


@pytest.fixture(name="snowflake_scd_data", scope="session")
def snowflake_scd_data_fixture(
    scd_data_tabular_source,
    user_entity,
    status_entity,
):
    """
    Fixture for a SlowlyChangingData in integration tests
    """
    name = "snowflake_scd_data"
    data = SlowlyChangingData.from_tabular_source(
        tabular_source=scd_data_tabular_source,
        name=name,
        natural_key_column="User ID",
        effective_timestamp_column="Effective Timestamp",
        surrogate_key_column="ID",
    )
    data.save()
    data = SlowlyChangingData.get(name)
    data["User ID"].as_entity(user_entity.name)
    data["User Status"].as_entity(status_entity.name)
    return data


@pytest.fixture(name="databricks_event_data", scope="session")
def databricks_event_data_fixture(
    databricks_session,
    databricks_feature_store,
    user_entity,
    product_action_entity,
    customer_entity,
):
    """Fixture for an EventData in integration tests"""
    _ = user_entity
    _ = product_action_entity
    _ = customer_entity
    event_data = create_transactions_event_data_from_feature_store(
        databricks_feature_store,
        database_name=databricks_session.featurebyte_catalog,
        schema_name=databricks_session.featurebyte_schema,
        table_name="TEST_TABLE",
        event_data_name="databricks_event_data",
    )
    return event_data


def get_data_source_from_request(request, valid_data_sources):
    """
    Helper function to get data source from request.

    Note that this currently just checks for snowflake. We can update this in the future when we start to add
    support for other data sources.
    """
    kind = "snowflake"
    if hasattr(request, "param"):
        kind = request.param
    assert kind in valid_data_sources
    return kind


@pytest.fixture(name="dimension_data", scope="session")
def dimension_data_fixture(request):
    """
    Parametrizable fixture for DimensionData (possible parameters: "snowflake")
    """
    _ = get_data_source_from_request(request, {"snowflake"})
    return request.getfixturevalue("snowflake_dimension_data")


@pytest.fixture(name="dimension_view", scope="session")
def dimension_view_fixture(request):
    """
    Parametrizable fixture for DimensionView (possible parameters: "snowflake")
    """
    _ = get_data_source_from_request(request, {"snowflake"})
    dimension_data = request.getfixturevalue("snowflake_dimension_data")
    return DimensionView.from_dimension_data(dimension_data)


@pytest.fixture(name="scd_data", scope="session")
def scd_data_fixture(request):
    """
    Parametrizable fixture for DimensionData (possible parameters: "snowflake")
    """
    _ = get_data_source_from_request(request, {"snowflake"})
    return request.getfixturevalue("snowflake_scd_data")


@pytest.fixture(name="event_data", scope="session")
def event_data_fixture(request):
    """
    Parametrizable fixture for EventData (possible parameters: "snowflake", "databricks")
    """
    kind = get_data_source_from_request(request, {"snowflake", "databricks"})
    if kind == "snowflake":
        return request.getfixturevalue("snowflake_event_data")
    return request.getfixturevalue("databricks_event_data")


@pytest.fixture(name="item_data", scope="session")
def item_data_fixture(request):
    """
    Fixture for ItemData
    """
    _ = get_data_source_from_request(request, {"snowflake"})
    return request.getfixturevalue("snowflake_item_data")


@pytest.fixture(name="get_cred")
def get_get_cred(config):
    """
    Fixture to get a test get_credential
    """

    async def get_credential(user_id, feature_store_name):
        _ = user_id
        return config.credentials.get(feature_store_name)

    return get_credential
