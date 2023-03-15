"""
Common test fixtures used across files in integration directory
"""
from typing import AsyncIterator, Dict, List

import asyncio
import json
import os

# pylint: disable=too-many-lines
import shutil
import sqlite3
import tempfile
import textwrap
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
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

from featurebyte import DatabricksDetails, FeatureJobSetting, SnowflakeDetails
from featurebyte.api.dimension_table import DimensionTable
from featurebyte.api.entity import Entity
from featurebyte.api.event_table import EventTable
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.item_table import ItemTable
from featurebyte.api.scd_table import SCDTable
from featurebyte.app import app
from featurebyte.common.tile_util import tile_manager_from_session
from featurebyte.config import Configurations
from featurebyte.enum import InternalName, SourceType, StorageType
from featurebyte.feature_manager.manager import FeatureManager
from featurebyte.feature_manager.model import ExtendedFeatureListModel
from featurebyte.logger import logger
from featurebyte.models.feature import FeatureModel, FeatureReadiness
from featurebyte.models.feature_list import FeatureListStatus
from featurebyte.persistent.mongo import MongoDB
from featurebyte.query_graph.node.schema import SparkDetails, SQLiteDetails, TableDetails
from featurebyte.session.manager import SessionManager
from featurebyte.tile.snowflake_tile import TileSpec

# Static testing mongodb connection from docker/test/docker-compose.yml
MONGO_CONNECTION = "mongodb://localhost:27021,localhost:27022/?replicaSet=rs0"


def pytest_collection_modifyitems(config, items):
    """
    Re-order the tests such that tests using the same source_type are run together. This prevents
    premature tear down of fixtures that depend on source_type. This is because if a fixture depends
    on the source_type fixture, at any one time there can be only one single instance of it with a
    specific source_type.

    For example, we need to ensure that all tests using Snowflake are run first, then run the Spark
    tests, etc. Before the Spark tests run, pytest will tear down all the Snowflake fixtures.

    Also handles filtering of integration tests by source_type (specified via --source-types option
    as a comma separated list).
    """

    source_types_config = config.getoption("source_types")
    if source_types_config is not None:
        filtered_source_types = source_types_config.split(",")
        filtered_source_types = {source_type.strip() for source_type in filtered_source_types}
    else:
        filtered_source_types = None

    def get_source_type_from_item(item):
        if hasattr(item, "callspec"):
            source_type = item.callspec.params.get("source_type")
            if source_type is not None:
                return source_type
        return None

    def get_sorting_key(entry):

        index, item = entry
        extra_ordering_key = ""

        # check for the source_type parameter if available
        source_type = get_source_type_from_item(item)
        if source_type is not None:
            extra_ordering_key = source_type

        # include index as the sorting key to preserve the original ordering
        return extra_ordering_key, index

    def filter_items_by_source_types(all_items):

        if not filtered_source_types:
            return all_items

        filtered_items = []
        for item in all_items:
            source_type = get_source_type_from_item(item)
            if source_type is None:
                source_type = "none"
            if source_type in filtered_source_types:
                filtered_items.append(item)

        return filtered_items

    # re-order the tests
    _ = config
    sorted_entries = sorted(enumerate(items), key=get_sorting_key)
    sorted_items = [entry[1] for entry in sorted_entries]
    filtered_items = filter_items_by_source_types(sorted_items)
    items[:] = filtered_items


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
    try:
        loop = asyncio.get_event_loop()
        yield loop
        loop.close()
    except Exception as e:  # pylint: disable=broad-except
        if "there is no current event loop in thread" in str(e):
            logger.exception(
                f"no event loop found. explicitly recreating and resetting new event loop.\n"
                f"previous error: {str(e)}"
            )
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            yield loop
            loop.close()


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
    database_name = f"test_{ObjectId()}"
    client = pymongo.MongoClient(MONGO_CONNECTION)
    persistent = MongoDB(uri=MONGO_CONNECTION, database=database_name)
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


@pytest.fixture(name="source_type", scope="session", params=["snowflake", "spark"])
def source_type_fixture(request):
    """
    Fixture for the source_type parameter used to create all the other fixtures

    This is the fixture that controls what sources a test will run against. By default, a test will
    run on all the sources listed in "params" above. If it is desired that a test only runs on a
    different set of sources, it can be customised at test time using parametrize().

    Example 1
    ---------

    def my_test(event_data):
        # event_data will be instantiated using Snowflake, Spark, ... etc. my_test will be executed
        # multiple times, each time with an EventData from a different source.
        ...

    Example 2
    ----------

    @pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
    def my_test(event_data):
        # event_data will be instantiated using Snowflake as the source and my_test only runs once
        ...

    """
    return request.param


@pytest.fixture(name="feature_store_details", scope="session")
def feature_store_details_fixture(source_type, sqlite_filename):
    """
    Fixture for a BaseDatabaseDetails specific to source_type
    """

    if source_type == "snowflake":
        schema_name = os.getenv("SNOWFLAKE_SCHEMA_FEATUREBYTE")
        temp_schema_name = f"{schema_name}_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
        return SnowflakeDetails(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            sf_schema=temp_schema_name,
            database=os.getenv("SNOWFLAKE_DATABASE"),
        )

    if source_type == "databricks":
        schema_name = os.getenv("DATABRICKS_SCHEMA_FEATUREBYTE")
        temp_schema_name = f"{schema_name}_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
        return DatabricksDetails(
            server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
            http_path=os.getenv("DATABRICKS_HTTP_PATH"),
            featurebyte_catalog=os.getenv("DATABRICKS_CATALOG"),
            featurebyte_schema=temp_schema_name,
        )

    if source_type == "spark":
        schema_name = "featurebyte"
        temp_schema_name = f"{schema_name}_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
        return SparkDetails(
            host="localhost",
            port=10000,
            http_path="cliservice",
            use_http_transport=False,
            storage_type=StorageType.FILE,
            storage_url=f"~/.spark/data/staging/{temp_schema_name}",
            storage_spark_url=f"file:///opt/spark/data/derby/staging/{temp_schema_name}",
            featurebyte_catalog="spark_catalog",
            featurebyte_schema=temp_schema_name,
        )

    if source_type == "sqlite":
        return SQLiteDetails(filename=sqlite_filename)

    return NotImplementedError(f"Unexpected source_type: {source_type}")


@pytest.fixture(name="feature_store_name", scope="session")
def feature_store_name_fixture(source_type):
    """
    Feature store name fixture
    """
    return f"{source_type}_featurestore"


@pytest.fixture(name="feature_store", scope="session")
def feature_store_fixture(
    source_type, feature_store_name, feature_store_details, mock_get_persistent
):
    """
    Feature store fixture
    """
    _ = mock_get_persistent
    feature_store = FeatureStore.create(
        name=feature_store_name,
        source_type=source_type,
        details=feature_store_details,
    )
    yield feature_store


@pytest.fixture(name="data_source", scope="session")
def data_source_fixture(feature_store):
    """
    Data source fixture
    """
    return feature_store.get_data_source()


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
    product_actions = ["detail", "àdd", "purchase", "rëmove", None]
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
            "ëvent_timestamp": event_timestamps,
            "created_at": pd.date_range("2001-01-01", freq="1min", periods=row_number),
            "cust_id": rng.randint(1, 1000, row_number),
            "üser id": rng.randint(1, 10, row_number),
            "product_action": rng.choice(product_actions, row_number),
            "session_id": rng.randint(100, 1000, row_number),
        }
    )
    amount = (rng.rand(row_number) * 100).round(2)
    amount[::5] = np.nan
    data["àmount"] = amount
    data["created_at"] += rng.randint(1, 100, row_number).cumsum() * pd.Timedelta(seconds=1)
    data["created_at"] = data["created_at"].astype(int)
    data["session_id"] = data["session_id"].sample(frac=1.0, random_state=0).reset_index(drop=True)

    # add some second-level variation to the event timestamp
    data["ëvent_timestamp"] += rng.randint(0, 3600, len(timestamps)) * pd.Timedelta(seconds=1)
    # exclude any nanosecond components since it is not supported
    data["ëvent_timestamp"] = data["ëvent_timestamp"].dt.floor("us")
    # add timezone offset
    offsets = rng.choice(range(24), size=data["ëvent_timestamp"].shape[0])
    data["ëvent_timestamp"] = data["ëvent_timestamp"] + pd.Series(offsets) * pd.Timedelta(hours=1)
    data["ëvent_timestamp"] = pd.to_datetime(
        data["ëvent_timestamp"].astype(str) + [f"+{i:02d}:00" for i in offsets]
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


@pytest.fixture(name="items_dataframe", scope="session")
def items_dataframe_fixture(transaction_data_upper_case):
    """
    DataFrame fixture with item based data corresponding to the transaction data
    """
    rng = np.random.RandomState(0)  # pylint: disable=no-member
    data = defaultdict(list)
    item_types = [f"type_{i}" for i in range(100)]

    def generate_items_for_transaction(transaction_row):
        order_id = transaction_row["TRANSACTION_ID"]
        num_items = rng.randint(1, 10)
        selected_item_types = rng.choice(item_types, num_items, replace=False)
        data["order_id"].extend([order_id] * num_items)
        data["item_type"].extend(selected_item_types)

    for _, row in transaction_data_upper_case.iterrows():
        generate_items_for_transaction(row)

    df_items = pd.DataFrame(data)
    item_ids = pd.Series([f"item_{i}" for i in range(df_items.shape[0])])
    df_items.insert(1, "item_id", item_ids)
    return df_items


@pytest.fixture(name="item_ids", scope="session")
def item_ids_fixture(items_dataframe):
    """
    Fixture to get item IDs used in test data.
    """
    return items_dataframe["item_id"].tolist()


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

    natural_key_values = sorted(transaction_data["üser id"].unique())
    dates = pd.to_datetime(transaction_data["ëvent_timestamp"], utc=True).dt.floor("d")
    effective_timestamp_values = pd.date_range(dates.min(), dates.max())
    # Add variations at hour level
    effective_timestamp_values += pd.to_timedelta(
        rng.randint(0, 24, len(effective_timestamp_values)), unit="h"
    )
    values = [f"STÀTUS_CODE_{i}" for i in range(50)]

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
            ["TRANSACTION_ID", "ËVENT_TIMESTAMP", "ÜSER ID", "CUST_ID", "PRODUCT_ACTION"]
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


@pytest_asyncio.fixture(name="dataset_registration_helper", scope="session")
async def datasets_registration_helper_fixture(
    transaction_data_upper_case,
    items_dataframe,
    dimension_dataframe,
    dimension_data_table_name,
    scd_dataframe,
    scd_data_table_name,
):
    """
    Reusable helper to register all the datasets used in integration tests
    """

    class DatasetsRegistrationHelper:
        """
        Helper to register datasets
        """

        def __init__(self):
            self.datasets: Dict[str, pd.DataFrame] = {}

        def add_table(self, table_name: str, df: pd.DataFrame):
            """
            Register a table in memory (not yet written to database)

            Parameters
            ----------
            table_name: str
                Table name
            df: DataFrame
                Pandas DataFrame
            """
            self.datasets[table_name] = df.copy()

        async def register_datasets(self, session):
            """
            Register all the added tables to data warehouse

            Parameters
            ----------
            session: BaseSession
                Session object
            """
            for table_name, df in self.datasets.items():
                await session.register_table(table_name, df, temporary=False)

        @property
        def table_names(self) -> List[str]:
            """
            List of table names registered

            Returns
            -------
            list[str]
            """
            return list(self.datasets.keys())

    helper = DatasetsRegistrationHelper()

    # Event table
    helper.add_table("TEST_TABLE", transaction_data_upper_case)

    # Item table
    helper.add_table("ITEM_DATA_TABLE", items_dataframe)

    # Dimension table
    helper.add_table(dimension_data_table_name, dimension_dataframe)

    # SCD table
    helper.add_table(scd_data_table_name, scd_dataframe)

    # Tile table for tile manager integration tests
    df_tiles = pd.read_csv(os.path.join(os.path.dirname(__file__), "tile", "tile_data.csv"))
    # Tile table timestamps are TZ-naive
    df_tiles[InternalName.TILE_START_DATE] = pd.to_datetime(
        df_tiles[InternalName.TILE_START_DATE], utc=True
    ).dt.tz_localize(None)
    helper.add_table("TEMP_TABLE", df_tiles)

    yield helper


@pytest_asyncio.fixture(name="session", scope="session")
async def session_fixture(source_type, session_manager, dataset_registration_helper, feature_store):
    """
    Fixture for a BaseSession based on source_type
    """
    session = await session_manager.get_session(feature_store)

    await dataset_registration_helper.register_datasets(session)

    if source_type == "databricks":
        await session.execute_query(
            """
            CREATE TABLE TEST_TABLE
            USING CSV
            OPTIONS (header "true", inferSchema "true")
            LOCATION 'dbfs:/FileStore/tables/transactions_data_upper_case_2022_10_17.csv';
            """
        )

    yield session

    if source_type == "snowflake":
        await session.execute_query(f"DROP SCHEMA IF EXISTS {session.schema_name}")

    if source_type == "databricks":
        await session.execute_query(f"DROP SCHEMA IF EXISTS {session.schema_name} CASCADE")

    if source_type == "spark":
        await session.execute_query(f"DROP SCHEMA IF EXISTS {session.schema_name} CASCADE")
        # clean up storage
        shutil.rmtree(Path(feature_store.details.storage_url).expanduser())


def create_generic_tile_spec():
    """
    Helper to create a generic tile_spec
    """
    col_names_list = [InternalName.TILE_START_DATE, "PRODUCT_ACTION", "CUST_ID", "VALUE"]
    col_names = ",".join(col_names_list)
    table_name = "TEMP_TABLE"
    start = InternalName.TILE_START_DATE_SQL_PLACEHOLDER
    end = InternalName.TILE_END_DATE_SQL_PLACEHOLDER

    tile_sql = f"SELECT {col_names} FROM {table_name} WHERE {InternalName.TILE_START_DATE} >= {start} and {InternalName.TILE_START_DATE} < {end}"
    tile_id = "TILE_ID1"
    aggregation_id = "agg_id1"

    return TileSpec(
        time_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        tile_sql=tile_sql,
        column_names=col_names_list,
        entity_column_names=["PRODUCT_ACTION", "CUST_ID"],
        value_column_names=["VALUE"],
        value_column_types=["FLOAT"],
        tile_id=tile_id,
        aggregation_id=aggregation_id,
    )


@asynccontextmanager
async def create_snowflake_tile_spec(session):
    """
    Helper to create a snowflake tile_spec with snowflake specific clean up
    """
    assert session.source_type == "snowflake"
    tile_spec = None
    try:
        tile_spec = create_generic_tile_spec()
        yield tile_spec
    finally:
        if tile_spec is not None:
            await session.execute_query("DELETE FROM TILE_REGISTRY")
            await session.execute_query(
                f"DROP TABLE IF EXISTS {tile_spec.aggregation_id}_ENTITY_TRACKER"
            )
            await session.execute_query(f"DROP TABLE IF EXISTS {tile_spec.tile_id}")
            await session.execute_query(
                f"DROP TASK IF EXISTS SHELL_TASK_{tile_spec.aggregation_id}_ONLINE"
            )
            await session.execute_query(
                f"DROP TASK IF EXISTS SHELL_TASK_{tile_spec.aggregation_id}_OFFLINE"
            )


@asynccontextmanager
async def create_spark_tile_specs(session):
    """
    Helper to create a snowflake tile_spec with spark specific clean up
    """
    _ = session
    yield create_generic_tile_spec()


@asynccontextmanager
async def create_databricks_tile_spec(session):
    """
    Pytest Fixture for TileSnowflake instance
    """
    assert session.source_type == "databricks"
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

    try:
        yield tile_spec
    finally:
        await session.execute_query(f"DROP TABLE IF EXISTS {tile_id}")


@pytest_asyncio.fixture(name="tile_spec")
async def tile_spec_fixture(session):
    """
    Pytest Fixture for TileSnowflake instance
    """
    creator = None
    if session.source_type == "snowflake":
        creator = create_snowflake_tile_spec
    if session.source_type == "spark":
        creator = create_spark_tile_specs
    elif session.source_type == "databricks":
        creator = create_databricks_tile_spec

    assert creator is not None, f"tile_spec fixture does not support {session.source_type}"

    async with creator(session) as created_tile_spec:
        yield created_tile_spec


@pytest.fixture
def tile_manager(session):
    """
    Tile Manager fixture
    """
    return tile_manager_from_session(session=session)


@pytest.fixture(name="feature_model_dict")
def feature_model_dict_feature(test_dir):
    """Fixture for a Feature dict"""
    feature_fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_sum_30m.json")
    with open(feature_fixture_path) as file_handle:
        return json.load(file_handle)


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
def feature_manager(session):
    """
    Feature Manager fixture
    """
    return FeatureManager(session=session)


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
    entity = Entity(name="User", serving_names=["üser id"])
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


def create_transactions_event_data_from_data_source(
    data_source, database_name, schema_name, table_name, event_data_name
):
    """
    Helper function to create an EventData with the given feature store
    """
    available_tables = data_source.list_tables(
        database_name=database_name,
        schema_name=schema_name,
    )
    # check table exists (case-insensitive since some data warehouses change the casing)
    available_tables = [x.upper() for x in available_tables]
    assert table_name.upper() in available_tables

    database_table = data_source.get_table(
        database_name=database_name,
        schema_name=schema_name,
        table_name=table_name,
    )
    expected_dtypes = pd.Series(
        {
            "ËVENT_TIMESTAMP": "TIMESTAMP_TZ"
            if data_source.type == SourceType.SNOWFLAKE
            else "TIMESTAMP",
            "CREATED_AT": "INT",
            "CUST_ID": "INT",
            "ÜSER ID": "INT",
            "PRODUCT_ACTION": "VARCHAR",
            "SESSION_ID": "INT",
            "ÀMOUNT": "FLOAT",
            "TRANSACTION_ID": "VARCHAR",
        }
    )
    pd.testing.assert_series_equal(expected_dtypes, database_table.dtypes)
    event_data = EventTable.from_tabular_source(
        tabular_source=database_table,
        name=event_data_name,
        event_id_column="TRANSACTION_ID",
        event_timestamp_column="ËVENT_TIMESTAMP",
    )
    event_data.update_default_feature_job_setting(
        feature_job_setting=FeatureJobSetting(
            blind_spot="30m", frequency="1h", time_modulo_frequency="30m"
        )
    )
    event_data["TRANSACTION_ID"].as_entity("Order")
    event_data["ÜSER ID"].as_entity("User")
    event_data["PRODUCT_ACTION"].as_entity("ProductAction")
    event_data["CUST_ID"].as_entity("Customer")
    event_data.save()
    event_data = EventTable.get(event_data_name)
    return event_data


@pytest.fixture(name="event_data_name", scope="session")
def event_data_name_fixture(source_type):
    """
    Fixture for the EventData name
    """
    return f"{source_type}_event_data"


@pytest.fixture(name="event_data", scope="session")
def event_data_fixture(
    session,
    data_source,
    event_data_name,
    user_entity,
    product_action_entity,
    customer_entity,
    order_entity,
):
    """
    Fixture for an EventData in integration tests
    """
    _ = user_entity
    _ = product_action_entity
    _ = customer_entity
    _ = order_entity
    event_data = create_transactions_event_data_from_data_source(
        data_source=data_source,
        database_name=session.database_name,
        schema_name=session.schema_name,
        table_name="TEST_TABLE",
        event_data_name=event_data_name,
    )
    return event_data


@pytest.fixture(name="item_data_name", scope="session")
def item_data_name_fixture(source_type):
    """
    Fixture for the ItemTable name
    """
    return f"{source_type}_item_data"


@pytest.fixture(name="item_data", scope="session")
def item_data_fixture(
    session,
    data_source,
    item_data_name,
    event_data,
    order_entity,
    item_entity,
):
    """
    Fixture for an ItemTable in integration tests
    """
    database_table = data_source.get_table(
        database_name=session.database_name,
        schema_name=session.schema_name,
        table_name="ITEM_DATA_TABLE",
    )
    item_data = ItemTable.from_tabular_source(
        tabular_source=database_table,
        name=item_data_name,
        event_id_column="order_id",
        item_id_column="item_id",
        event_data_name=event_data.name,
    )
    item_data.save()
    item_data = ItemTable.get(item_data_name)
    item_data["order_id"].as_entity(order_entity.name)
    item_data["item_id"].as_entity(item_entity.name)
    return item_data


@pytest.fixture(name="dimension_data_name", scope="session")
def dimension_data_name_fixture(source_type):
    """
    Fixture for the DimensionTable name
    """
    return f"{source_type}_dimension_data"


@pytest.fixture(name="dimension_data", scope="session")
def dimension_data_fixture(
    session,
    data_source,
    dimension_data_table_name,
    dimension_data_name,
    item_entity,
):
    """
    Fixture for a DimensionTable in integration tests
    """
    database_table = data_source.get_table(
        database_name=session.database_name,
        schema_name=session.schema_name,
        table_name=dimension_data_table_name,
    )
    dimension_data = DimensionTable.from_tabular_source(
        tabular_source=database_table,
        name=dimension_data_name,
        dimension_id_column="item_id",
    )
    dimension_data.save()
    dimension_data = DimensionTable.get(dimension_data_name)
    dimension_data["item_id"].as_entity(item_entity.name)
    return dimension_data


@pytest.fixture(name="scd_data_tabular_source", scope="session")
def scd_data_tabular_source_fixture(
    session,
    data_source,
    scd_data_table_name,
):
    """
    Fixture for scd data tabular source
    """
    database_table = data_source.get_table(
        database_name=session.database_name,
        schema_name=session.schema_name,
        table_name=scd_data_table_name,
    )
    return database_table


@pytest.fixture(name="scd_data_name", scope="session")
def scd_data_name_fixture(source_type):
    """
    Fixture for the SlowlyChangingData name
    """
    return f"{source_type}_scd_data"


@pytest.fixture(name="scd_data", scope="session")
def scd_data_fixture(
    scd_data_tabular_source,
    scd_data_name,
    user_entity,
    status_entity,
):
    """
    Fixture for a SlowlyChangingData in integration tests
    """
    data = SCDTable.from_tabular_source(
        tabular_source=scd_data_tabular_source,
        name=scd_data_name,
        natural_key_column="User ID",
        effective_timestamp_column="Effective Timestamp",
        surrogate_key_column="ID",
    )
    data.save()
    data = SCDTable.get(scd_data_name)
    data["User ID"].as_entity(user_entity.name)
    data["User Status"].as_entity(status_entity.name)
    return data


@pytest.fixture(name="dimension_view", scope="session")
def dimension_view_fixture(dimension_data):
    """
    Fixture for a DimensionView
    """
    return dimension_data.get_view()


@pytest.fixture(name="get_cred")
def get_get_cred(config):
    """
    Fixture to get a test get_credential
    """

    async def get_credential(user_id, feature_store_name):
        _ = user_id
        return config.credentials.get(feature_store_name)

    return get_credential
