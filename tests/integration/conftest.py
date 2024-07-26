"""
Common test fixtures used across files in integration directory
"""

# pylint: disable=too-many-lines
from typing import Dict, List, cast

import asyncio
import json
import os
import shutil
import sqlite3
import tempfile
import textwrap
import traceback
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from unittest import mock
from unittest.mock import Mock, patch
from uuid import UUID, uuid4

import numpy as np
import pandas as pd
import pymongo
import pytest
import pytest_asyncio
import redis
import yaml
from bson import ObjectId
from databricks import sql as databricks_sql
from fastapi.testclient import TestClient
from motor.motor_asyncio import AsyncIOMotorClient

from featurebyte import (
    Catalog,
    Configurations,
    DatabricksDetails,
    FeatureGroup,
    FeatureJobSetting,
    OnlineStore,
    RedisOnlineStoreDetails,
    SnowflakeDetails,
)
from featurebyte.api.entity import Entity
from featurebyte.api.feature_store import FeatureStore
from featurebyte.app import app
from featurebyte.enum import InternalName, SourceType, StorageType
from featurebyte.logging import get_logger
from featurebyte.models.base import DEFAULT_CATALOG_ID, User
from featurebyte.models.credential import (
    AccessTokenCredential,
    CredentialModel,
    UsernamePasswordCredential,
)
from featurebyte.models.task import Task as TaskModel
from featurebyte.models.tile import TileSpec
from featurebyte.persistent.mongo import MongoDB
from featurebyte.query_graph.node.schema import DatabricksUnityDetails, SparkDetails, SQLiteDetails
from featurebyte.routes.lazy_app_container import LazyAppContainer
from featurebyte.routes.registry import app_container_config
from featurebyte.schema.task import TaskStatus
from featurebyte.schema.worker.task.base import BaseTaskPayload
from featurebyte.service.online_store_compute_query_service import OnlineStoreComputeQueryService
from featurebyte.service.online_store_table_version import OnlineStoreTableVersionService
from featurebyte.service.task_manager import TaskManager
from featurebyte.session.manager import SessionManager
from featurebyte.storage import LocalStorage
from featurebyte.worker import get_celery
from featurebyte.worker.registry import TASK_REGISTRY_MAP
from tests.source_types import SNOWFLAKE_SPARK_DATABRICKS_UNITY

MONGO_CONNECTION = "mongodb://localhost:37017/?replicaSet=rs0"
TEST_REDIS_URI = "redis://localhost:36379"


logger = get_logger(__name__)


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
        return extra_ordering_key, item.path, index

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


@pytest.fixture(name="credentials_mapping", scope="session")
def credentials_mapping_fixture():
    """
    Credentials for integration testing
    """
    username_password = CredentialModel(
        name="snowflake_featurestore",
        feature_store_id=ObjectId(),
        database_credential=UsernamePasswordCredential(
            username=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
        ),
    )
    return {
        "snowflake_featurestore": username_password,
        "snowflake_featurestore_invalid_because_same_schema_a": username_password,
        "snowflake_featurestore_invalid_because_same_schema_b": username_password,
        "snowflake_featurestore_unreachable": username_password,
        "snowflake_featurestore_wrong_creds": CredentialModel(
            name="snowflake_featurestore",
            feature_store_id=ObjectId(),
            database_credential=UsernamePasswordCredential(
                username="wrong-user",
                password="wrong-password",
            ),
        ),
        "sqlite_datasource": CredentialModel(
            name="sqlite_datasource",
            feature_store_id=ObjectId(),
        ),
        "databricks_featurestore": CredentialModel(
            name="databricks_featurestore",
            feature_store_id=ObjectId(),
            database_credential=AccessTokenCredential(
                access_token=os.getenv("DATABRICKS_ACCESS_TOKEN", ""),
            ),
        ),
        "databricks_unity_featurestore": CredentialModel(
            name="databricks_unity_featurestore",
            feature_store_id=ObjectId(),
            database_credential=AccessTokenCredential(
                access_token=os.getenv("DATABRICKS_ACCESS_TOKEN", ""),
            ),
        ),
        "spark_featurestore": CredentialModel(
            name="spark_featurestore",
            feature_store_id=ObjectId(),
        ),
    }


@pytest.fixture(name="storage", scope="session")
def storage_fixture():
    """
    Storage object fixture
    """
    with tempfile.TemporaryDirectory(suffix=str(ObjectId())) as tempdir:
        yield LocalStorage(base_path=Path(tempdir))


@pytest.fixture(name="always_patch_app_get_storage", scope="session", autouse=True)
def always_patch_app_get_storage_fixture(storage):
    """
    Patch app.get_storage for all tests in this module
    """
    with patch("featurebyte.app.get_storage", return_value=storage):
        yield


@pytest.fixture(name="config", scope="session")
def config_fixture(storage):
    """
    Config object for integration testing
    """
    config_dict = {
        "profile": [
            {
                "name": "local",
                "api_url": "http://localhost:8080",
                "api_token": "token",
            }
        ],
        "default_profile": "local",
        "logging": {
            "level": "DEBUG",
        },
    }

    with tempfile.TemporaryDirectory() as tempdir:
        config_file_path = os.path.join(tempdir, "config.yaml")
        with open(config_file_path, "w") as file_handle:
            file_handle.write(yaml.dump(config_dict))
            file_handle.flush()
            with mock.patch("featurebyte.config.BaseAPIClient.request") as mock_request:
                with mock.patch("featurebyte.app.get_storage") as mock_get_storage:
                    mock_get_storage.return_value = storage
                    with TestClient(app) as client:

                        def wrapped_test_client_request_func(*args, stream=None, **kwargs):
                            _ = stream
                            response = client.request(*args, **kwargs)
                            response.iter_content = response.iter_bytes
                            return response

                        mock_request.side_effect = wrapped_test_client_request_func
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
                "no event loop found. explicitly recreating and resetting new event loop.",
                extra={"previous error": str(e)},
            )
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            yield loop
            loop.close()


@pytest.fixture(name="persistent", scope="session")
def persistent_fixture():
    """
    Mongo persistent fixture used by most of the integration tests
    """
    database_name = f"test_{ObjectId()}"
    client = AsyncIOMotorClient(MONGO_CONNECTION)
    client.get_io_loop = asyncio.get_running_loop
    persistent = MongoDB(uri=MONGO_CONNECTION, database=database_name, client=client)
    yield persistent
    client = AsyncIOMotorClient(MONGO_CONNECTION)
    client.drop_database(database_name)


@pytest.fixture(name="mongo_database_name")
def mongo_database_name():
    """
    Mongo database name used by integration tests
    """
    return f"test_{ObjectId()}"


def get_new_persistent(database_name):
    """
    Get a new persistent instance
    """
    client = pymongo.MongoClient(MONGO_CONNECTION)
    persistent = MongoDB(uri=MONGO_CONNECTION, database=database_name)
    return persistent, client


@pytest.fixture(name="mongo_persistent")
def mongo_persistent_fixture(mongo_database_name):
    """
    Mongo persistent fixture that uses a non-async client. Used by some integration tests that
    interact with the persistent directly.
    """
    persistent, client = get_new_persistent(mongo_database_name)
    yield persistent, client[mongo_database_name]
    client.drop_database(mongo_database_name)


@pytest.fixture(name="mock_get_persistent", scope="session")
def mock_get_persistent_fixture(persistent):
    """
    Mock get_persistent in featurebyte/app.py
    """
    with (
        mock.patch("featurebyte.app.MongoDBImpl") as mock_persistent,
        mock.patch("featurebyte.worker.task_executor.MongoDBImpl") as mock_persistent_worker,
    ):
        mock_persistent.return_value = persistent
        mock_persistent_worker.return_value = persistent
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


@pytest.fixture(name="source_type", scope="session", params=SNOWFLAKE_SPARK_DATABRICKS_UNITY)
def source_type_fixture(request):
    """
    Fixture for the source_type parameter used to create all the other fixtures

    This is the fixture that controls what sources a test will run against. By default, a test will
    run on all the sources listed in "params" above. If it is desired that a test only runs on a
    different set of sources, it can be customised at test time using parametrize().

    Example 1
    ---------

    def my_test(event_table):
        # event_table will be instantiated using Snowflake, Spark, ... etc. my_test will be executed
        # multiple times, each time with an EventTable from a different source.
        ...

    Example 2
    ----------

    @pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
    def my_test(event_table):
        # event_table will be instantiated using Snowflake as the source and my_test only runs once
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
            schema_name=temp_schema_name,
            database_name=os.getenv("SNOWFLAKE_DATABASE"),
            role_name="TESTING",
        )

    if source_type == "databricks":
        schema_name = os.getenv("DATABRICKS_SCHEMA_FEATUREBYTE")
        temp_schema_name = f"{schema_name}_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
        return DatabricksDetails(
            host=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
            http_path=os.getenv("DATABRICKS_HTTP_PATH"),
            catalog_name=os.getenv("DATABRICKS_CATALOG"),
            schema_name=temp_schema_name,
            storage_path=f"dbfs:/FileStore/{temp_schema_name}",
        )

    if source_type == "databricks_unity":
        schema_name = os.getenv("DATABRICKS_SCHEMA_FEATUREBYTE")
        temp_schema_name = f"{schema_name}_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
        return DatabricksUnityDetails(
            host=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
            http_path=os.getenv("DATABRICKS_UNITY_HTTP_PATH"),
            catalog_name=os.getenv("DATABRICKS_UNITY_CATALOG"),
            schema_name=temp_schema_name,
            group_name="developers",
        )

    if source_type == "spark":
        schema_name = "featurebyte"
        temp_schema_name = f"{schema_name}_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
        return SparkDetails(
            host="localhost",
            port=10009,
            storage_type=StorageType.FILE,
            storage_url=f"~/.spark/data/staging/{temp_schema_name}",
            storage_path=f"file:///opt/spark/data/derby/staging/{temp_schema_name}",
            catalog_name="spark_catalog",
            schema_name=temp_schema_name,
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


@pytest.fixture(name="feature_store_credential", scope="session")
def feature_store_credential_fixture(feature_store_name, credentials_mapping):
    """
    Fixture for a CredentialModel specific to source_type
    """
    return credentials_mapping.get(feature_store_name)


@pytest.fixture(name="data_warehouse_initialization", scope="session")
def data_warehouse_initialization_fixture(
    source_type,
    feature_store_details,
    feature_store_credential,
):
    """
    Data warehouse initialization fixture
    """
    if source_type in {SourceType.DATABRICKS, SourceType.DATABRICKS_UNITY}:
        # wait for databricks compute cluster to be ready
        databricks_details = cast(DatabricksDetails, feature_store_details)
        databricks_sql.connect(
            server_hostname=databricks_details.host,
            http_path=databricks_details.http_path,
            access_token=feature_store_credential.database_credential.access_token,
            catalog=databricks_details.catalog_name,
            schema=databricks_details.schema_name,
        )


@pytest.fixture(name="feature_store", scope="session")
def feature_store_fixture(
    source_type,
    feature_store_name,
    feature_store_details,
    feature_store_credential,
    data_warehouse_initialization,
    mock_get_persistent,
):
    """
    Feature store fixture
    """
    _ = mock_get_persistent
    _ = data_warehouse_initialization
    feature_store = FeatureStore.create(
        name=feature_store_name,
        source_type=source_type,
        details=feature_store_details,
        database_credential=feature_store_credential.database_credential,
        storage_credential=feature_store_credential.storage_credential,
    )
    yield feature_store


@pytest.fixture(name="data_source", scope="session")
def data_source_fixture(catalog):
    """
    Data source fixture
    """
    return catalog.get_data_source()


@pytest.fixture(name="catalog", scope="session")
def catalog_fixture(feature_store, online_store):
    """
    Catalog fixture
    """
    return Catalog.create(
        name="default",
        feature_store_name=feature_store.name,
        online_store_name=online_store.name,
    )


@pytest.fixture(name="mock_config_path_env", scope="session")
def mock_config_path_env_fixture(config):
    """Override default config path for all API tests"""
    with mock.patch.dict(
        os.environ,
        {
            "FEATUREBYTE_HOME": str(os.path.dirname(config.config_file_path)),
            "FEATUREBYTE_GRAPH_CLEAR_PERIOD": "1000",
        },
    ):
        yield


@pytest.fixture(autouse=True, scope="session")
def mock_settings_env_vars(mock_config_path_env, mock_get_persistent):
    """Use these fixtures for all tests"""
    _ = mock_config_path_env, mock_get_persistent
    yield


@pytest.fixture(name="transaction_data", scope="session")
def transaction_dataframe(source_type):
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
    offsets = rng.choice(range(-18, 18, 1), size=data["ëvent_timestamp"].shape[0])
    data["ëvent_timestamp"] = data["ëvent_timestamp"] + pd.Series(offsets) * pd.Timedelta(hours=1)
    formatted_offsets = [f"{i:+03d}:00" for i in offsets]
    data["ëvent_timestamp"] = pd.to_datetime(
        data["ëvent_timestamp"].astype(str) + formatted_offsets
    )
    data["tz_offset"] = formatted_offsets
    data["transaction_id"] = [f"T{i}" for i in range(data.shape[0])]

    if source_type != "sqlite":
        data["embedding_array"] = [rng.random(10).tolist() for _ in range(row_number)]
        data["array"] = [rng.random(rng.randint(5, 10)).tolist() for _ in range(row_number)]
        data["flat_dict"] = [
            {"a": rng.randint(0, 10), "b": rng.randint(0, 10)} for _ in range(row_number)
        ]
        data["nested_dict"] = [
            {"a": {"b": rng.randint(0, 10)}, "c": rng.randint(0, 10)} for _ in range(row_number)
        ]

    yield data


@pytest.fixture(name="transaction_data_upper_case", scope="session")
def transaction_dataframe_upper_case(transaction_data):
    """
    Convert transaction table column names to upper case
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
    DataFrame fixture with item based table corresponding to the transaction table
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
    Fixture to get item IDs used in test table.
    """
    return items_dataframe["item_id"].tolist()


@pytest.fixture(name="dimension_dataframe", scope="session")
def dimension_dataframe_fixture(item_ids):
    """
    DataFrame fixture with dimension table corresponding to items.
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


@pytest.fixture(name="observation_table_dataframe", scope="session")
def observation_table_dataframe_fixture(scd_dataframe):
    """
    DataFrame fixture for observation table
    """
    df = scd_dataframe[["Effective Timestamp", "User ID"]]
    df.rename({"Effective Timestamp": "POINT_IN_TIME"}, axis=1, inplace=True)
    return df


@pytest.fixture(scope="session")
def observation_set(transaction_data_upper_case):
    """
    Fixture for observation set
    """
    # Sample training time points from historical table
    df = transaction_data_upper_case
    cols = ["ËVENT_TIMESTAMP", "ÜSER ID"]
    df = df[cols].drop_duplicates(cols)
    df = df.sample(1000, replace=False, random_state=0).reset_index(drop=True)
    df.rename({"ËVENT_TIMESTAMP": "POINT_IN_TIME"}, axis=1, inplace=True)

    # Add random spikes to point in time of some rows
    rng = np.random.RandomState(0)  # pylint: disable=no-member
    spike_mask = rng.randint(0, 2, len(df)).astype(bool)
    spike_shift = pd.to_timedelta(rng.randint(0, 3601, len(df)), unit="s")
    df.loc[spike_mask, "POINT_IN_TIME"] = (
        df.loc[spike_mask, "POINT_IN_TIME"] + spike_shift[spike_mask]
    )
    df = df.reset_index(drop=True)
    return df


@pytest.fixture(name="expected_joined_event_item_dataframe", scope="session")
def expected_joined_event_item_dataframe_fixture(transaction_data_upper_case, items_dataframe):
    """
    DataFrame fixture with the expected joined event and item table
    """
    df = pd.merge(
        transaction_data_upper_case[
            [
                "TRANSACTION_ID",
                "ËVENT_TIMESTAMP",
                "ÜSER ID",
                "CUST_ID",
                "PRODUCT_ACTION",
                "TZ_OFFSET",
            ]
        ],
        items_dataframe,
        left_on="TRANSACTION_ID",
        right_on="order_id",
    )
    return df


@pytest.fixture(name="sqlite_filename", scope="session")
def sqlite_filename_fixture(transaction_data, source_type):
    """
    Create SQLite database file with table for testing
    """
    if source_type == "sqlite":
        with tempfile.NamedTemporaryFile() as file_handle:
            connection = sqlite3.connect(file_handle.name)
            transaction_data.to_sql(name="test_table", con=connection, index=False)
            connection.commit()
            yield file_handle.name
    else:
        yield


@pytest.fixture(name="session_manager", scope="session")
def get_session_manager(credentials_mapping):
    """
    Fixture to return a session manager with real login credentials
    """
    return SessionManager(credentials=credentials_mapping)


@pytest.fixture(name="get_session_callback", scope="session")
def get_session_callback_fixture(session_manager, feature_store):
    """
    Fixture to return a callback that returns a session object
    """

    async def callback():
        return await session_manager.get_session(feature_store)

    return callback


@pytest.fixture(name="dimension_data_table_name", scope="session")
def dimension_data_table_name_fixture():
    """
    Get the dimension table name used in integration tests.
    """
    return "DIMENSION_DATA_TABLE"


@pytest.fixture(name="scd_data_table_name", scope="session")
def scd_data_table_name_fixture():
    """
    Get the scd table name used in integration tests.
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
    observation_table_dataframe,
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
                await session.register_table(table_name, df)

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

    # Observation table
    helper.add_table("ORIGINAL_OBSERVATION_TABLE", observation_table_dataframe)

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

    yield session

    if source_type == "snowflake":
        await session.execute_query(f"DROP SCHEMA IF EXISTS {session.schema_name}")

    if source_type == "databricks":
        await session.execute_query(f"DROP SCHEMA IF EXISTS {session.schema_name} CASCADE")
        session._dbfs_client.delete(path=session._storage_base_path, recursive=True)
    if source_type == "databricks_unity":
        await session.execute_query(f"DROP SCHEMA IF EXISTS {session.schema_name} CASCADE")

    if source_type == "spark":
        await session.execute_query(f"DROP SCHEMA IF EXISTS {session.schema_name} CASCADE")
        # clean up storage
        shutil.rmtree(Path(feature_store.details.storage_url).expanduser())


def create_generic_tile_spec():
    """
    Helper to create a generic tile_spec
    """
    col_names_list = ["INDEX", "PRODUCT_ACTION", "CUST_ID", "VALUE"]
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
        feature_store_id=ObjectId(),
        # Set a large windows since many tests assume that all the tiles in the tile_data.csv will
        # be selected, but this is now not necessarily the case since we will only select the tiles
        # that are within the window.
        windows=["3650d"],
    )


@asynccontextmanager
async def create_snowflake_tile_spec(session, tile_registry_service):
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
            async for doc in tile_registry_service.list_documents_as_dict_iterator({}):
                await tile_registry_service.delete_document(doc["_id"])
            await session.execute_query(
                f"DROP TABLE IF EXISTS {tile_spec.aggregation_id}_ENTITY_TRACKER"
            )
            await session.execute_query(f"DROP TABLE IF EXISTS {tile_spec.tile_id}")


@asynccontextmanager
async def create_spark_tile_specs(session, **kwargs):
    """
    Helper to create a snowflake tile_spec with spark specific clean up
    """
    _ = session
    _ = kwargs
    yield create_generic_tile_spec()


@pytest_asyncio.fixture(name="tile_spec")
async def tile_spec_fixture(session, tile_registry_service):
    """
    Pytest Fixture for TileSnowflake instance
    """
    creator = None
    if session.source_type == "snowflake":
        creator = create_snowflake_tile_spec
    if session.source_type in {"spark", "databricks", "databricks_unity"}:
        creator = create_spark_tile_specs

    assert creator is not None, f"tile_spec fixture does not support {session.source_type}"

    async with creator(session, tile_registry_service=tile_registry_service) as created_tile_spec:
        yield created_tile_spec


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


@pytest.fixture(name="user_entity", scope="session")
def user_entity_fixture(catalog):
    """
    Fixture for an Entity "User"
    """
    _ = catalog
    entity = Entity(
        _id=ObjectId("65b36715208ba8c23717d9a1"), name="User", serving_names=["üser id"]
    )
    entity.save()
    return entity


@pytest.fixture(name="product_action_entity", scope="session")
def product_action_entity_fixture(catalog):
    """
    Fixture for an Entity "ProductAction"
    """
    _ = catalog
    entity = Entity(
        _id=ObjectId("65b36715208ba8c23717d9a2"),
        name="ProductAction",
        serving_names=["PRODUCT_ACTION"],
    )
    entity.save()
    return entity


@pytest.fixture(name="customer_entity", scope="session")
def customer_entity_fixture(catalog):
    """
    Fixture for an Entity "Customer"
    """
    _ = catalog
    entity = Entity(
        _id=ObjectId("65b36715208ba8c23717d9a3"), name="Customer", serving_names=["cust_id"]
    )
    entity.save()
    return entity


@pytest.fixture(name="order_entity", scope="session")
def order_entity_fixture(catalog):
    """
    Fixture for an Entity "Order"
    """
    _ = catalog
    entity = Entity(
        _id=ObjectId("65b36715208ba8c23717d9a4"), name="Order", serving_names=["order_id"]
    )
    entity.save()
    return entity


@pytest.fixture(name="item_entity", scope="session")
def item_entity_fixture(catalog):
    """
    Fixture for an Entity "Item"
    """
    _ = catalog
    entity = Entity(
        _id=ObjectId("65b36715208ba8c23717d9a5"), name="Item", serving_names=["item_id"]
    )
    entity.save()
    return entity


@pytest.fixture(name="item_type_entity", scope="session")
def item_type_entity_fixture(catalog):
    """
    Fixture for an Entity "ItemType"
    """
    _ = catalog
    entity = Entity(
        _id=ObjectId("664ac92379c46110f9275db1"), name="ItemType", serving_names=["item_type"]
    )
    entity.save()
    return entity


@pytest.fixture(name="status_entity", scope="session")
def status_entity_fixture(catalog):
    """
    Fixture for an Entity "UserStatus"
    """
    _ = catalog
    entity = Entity(
        _id=ObjectId("65b36715208ba8c23717d9a6"), name="UserStatus", serving_names=["user_status"]
    )
    entity.save()
    return entity


def tag_entities_for_event_table(event_table):
    """
    Helper function to tag entities for the event table fixture
    """
    event_table["TRANSACTION_ID"].as_entity("Order")
    event_table["ÜSER ID"].as_entity("User")
    event_table["PRODUCT_ACTION"].as_entity("ProductAction")
    event_table["CUST_ID"].as_entity("Customer")


def create_transactions_event_table_from_data_source(
    data_source, database_name, schema_name, table_name, event_table_name
):
    """
    Helper function to create an EventTable with the given feature store
    """

    database_table = data_source.get_source_table(
        database_name=database_name,
        schema_name=schema_name,
        table_name=table_name,
    )
    expected_dtypes = pd.Series(
        {
            "ËVENT_TIMESTAMP": (
                "TIMESTAMP_TZ" if data_source.type == SourceType.SNOWFLAKE else "TIMESTAMP"
            ),
            "CREATED_AT": "INT",
            "CUST_ID": "INT",
            "ÜSER ID": "INT",
            "PRODUCT_ACTION": "VARCHAR",
            "SESSION_ID": "INT",
            "ÀMOUNT": "FLOAT",
            "TZ_OFFSET": "VARCHAR",
            "TRANSACTION_ID": "VARCHAR",
            "EMBEDDING_ARRAY": "ARRAY",
            "ARRAY": "ARRAY",
            "FLAT_DICT": "DICT",
            "NESTED_DICT": "DICT",
        }
    )
    pd.testing.assert_series_equal(expected_dtypes, database_table.dtypes)
    event_table = database_table.create_event_table(
        name=event_table_name,
        event_id_column="TRANSACTION_ID",
        event_timestamp_column="ËVENT_TIMESTAMP",
        event_timestamp_timezone_offset_column="TZ_OFFSET",
    )
    event_table.update_default_feature_job_setting(
        feature_job_setting=FeatureJobSetting(blind_spot="30m", period="1h", offset="30m")
    )
    tag_entities_for_event_table(event_table)
    return event_table


@pytest.fixture(name="event_table_name", scope="session")
def event_table_name_fixture(source_type):
    """
    Fixture for the EventTable name
    """
    return f"{source_type}_event_table"


@pytest.fixture(name="event_table", scope="session")
def event_table_fixture(
    session,
    data_source,
    event_table_name,
    user_entity,
    product_action_entity,
    customer_entity,
    order_entity,
    catalog,
):
    """
    Fixture for an EventTable in integration tests
    """
    _ = catalog
    _ = user_entity
    _ = product_action_entity
    _ = customer_entity
    _ = order_entity
    event_table = create_transactions_event_table_from_data_source(
        data_source=data_source,
        database_name=session.database_name,
        schema_name=session.schema_name,
        table_name="TEST_TABLE",
        event_table_name=event_table_name,
    )
    return event_table


@pytest.fixture(name="event_view")
def event_view_fixture(event_table):
    """Event view fixture"""
    event_view = event_table.get_view()
    assert event_view.columns == [
        "ËVENT_TIMESTAMP",
        "CREATED_AT",
        "CUST_ID",
        "ÜSER ID",
        "PRODUCT_ACTION",
        "SESSION_ID",
        "ÀMOUNT",
        "TZ_OFFSET",
        "TRANSACTION_ID",
        "EMBEDDING_ARRAY",
        "ARRAY",
        "FLAT_DICT",
        "NESTED_DICT",
    ]
    return event_view


@pytest.fixture(name="item_table_name", scope="session")
def item_table_name_fixture(source_type):
    """
    Fixture for the ItemTable name
    """
    return f"{source_type}_item_table"


def tag_entities_for_item_table(item_table):
    """
    Tag entities for the item table fixture
    """
    item_table["order_id"].as_entity("Order")
    item_table["item_id"].as_entity("Item")
    item_table["item_type"].as_entity("ItemType")


@pytest.fixture(name="item_table", scope="session")
def item_table_fixture(
    session,
    data_source,
    item_table_name,
    event_table,
    order_entity,
    item_entity,
    item_type_entity,
    catalog,
):
    """
    Fixture for an ItemTable in integration tests
    """
    _ = catalog
    _ = order_entity
    _ = item_entity
    _ = item_type_entity
    database_table = data_source.get_source_table(
        database_name=session.database_name,
        schema_name=session.schema_name,
        table_name="ITEM_DATA_TABLE",
    )
    item_table = database_table.create_item_table(
        name=item_table_name,
        event_id_column="order_id",
        item_id_column="item_id",
        event_table_name=event_table.name,
    )
    tag_entities_for_item_table(item_table)
    return item_table


@pytest.fixture(name="dimension_table_name", scope="session")
def dimension_table_name_fixture(source_type):
    """
    Fixture for the DimensionTable name
    """
    return f"{source_type}_dimension_table"


@pytest.fixture(name="dimension_table", scope="session")
def dimension_table_fixture(
    session,
    data_source,
    dimension_data_table_name,
    dimension_table_name,
    item_entity,
    catalog,
):
    """
    Fixture for a DimensionTable in integration tests
    """
    _ = catalog
    database_table = data_source.get_source_table(
        database_name=session.database_name,
        schema_name=session.schema_name,
        table_name=dimension_data_table_name,
    )
    dimension_table = database_table.create_dimension_table(
        name=dimension_table_name,
        dimension_id_column="item_id",
    )
    dimension_table["item_id"].as_entity(item_entity.name)
    return dimension_table


@pytest.fixture(name="scd_data_tabular_source", scope="session")
def scd_data_tabular_source_fixture(
    session,
    data_source,
    scd_data_table_name,
):
    """
    Fixture for scd table tabular source
    """
    database_table = data_source.get_source_table(
        database_name=session.database_name,
        schema_name=session.schema_name,
        table_name=scd_data_table_name,
    )
    return database_table


@pytest.fixture(name="scd_table_name", scope="session")
def scd_table_name_fixture(source_type):
    """
    Fixture for the SCDTable name
    """
    return f"{source_type}_scd_table"


def tag_entities_for_scd_table(scd_table):
    """
    Tag entities for scd table fixture
    """
    scd_table["User ID"].as_entity("User")
    scd_table["User Status"].as_entity("UserStatus")


@pytest.fixture(name="scd_table", scope="session")
def scd_table_fixture(
    scd_data_tabular_source,
    scd_table_name,
    user_entity,
    status_entity,
    catalog,
):
    """
    Fixture for a SCDTable in integration tests
    """
    _ = catalog
    _ = user_entity
    _ = status_entity
    scd_table = scd_data_tabular_source.create_scd_table(
        name=scd_table_name,
        natural_key_column="User ID",
        effective_timestamp_column="Effective Timestamp",
        surrogate_key_column="ID",
    )
    tag_entities_for_scd_table(scd_table)
    return scd_table


@pytest.fixture(name="dimension_view", scope="session")
def dimension_view_fixture(dimension_table):
    """
    Fixture for a DimensionView
    """
    return dimension_table.get_view()


@pytest.fixture(name="get_cred", scope="session")
def get_get_cred(credentials_mapping):
    """
    Fixture to get a test get_credential
    """

    async def get_credential(user_id, feature_store_name):
        _ = user_id
        return credentials_mapping.get(feature_store_name)

    return get_credential


@pytest.fixture(autouse=True, scope="module")
def mock_task_manager(request, persistent, storage):
    """
    Mock celery task manager for testing
    """
    if request.module.__name__ == "test_task_manager":
        yield
    else:
        task_status = {}
        with patch("featurebyte.service.task_manager.TaskManager.submit") as mock_submit:

            async def submit(payload: BaseTaskPayload, **kwargs):
                kwargs = payload.json_dict()
                kwargs["task_output_path"] = payload.task_output_path
                task_id = str(uuid4())
                user = User(id=kwargs.get("user_id"))
                instance_map = {
                    "user": user,
                    "persistent": persistent,
                    "celery": Mock(),
                    "redis": redis.from_url(TEST_REDIS_URI),
                    "storage": storage,
                    "catalog_id": payload.catalog_id,
                }
                app_container = LazyAppContainer(
                    app_container_config=app_container_config, instance_map=instance_map
                )
                app_container.override_instance_for_test("task_id", UUID(task_id))
                app_container.override_instance_for_test("progress", Mock())
                task = app_container.get(TASK_REGISTRY_MAP[payload.command])
                try:
                    task_payload = task.get_payload_obj(kwargs)
                    await task.execute(task_payload)
                    status = TaskStatus.SUCCESS
                    traceback_info = None
                except Exception:  # pylint: disable=broad-except
                    status = TaskStatus.FAILURE
                    traceback_info = traceback.format_exc()

                task_status[task_id] = status

                # insert task into db manually since we are mocking celery
                task = TaskModel(
                    _id=task_id,
                    status=status,
                    result="",
                    children=[],
                    date_done=datetime.utcnow(),
                    name=payload.command,
                    args=[],
                    kwargs=kwargs,
                    worker="worker",
                    retries=0,
                    queue="default",
                    traceback=traceback_info,
                )
                document = task.dict(by_alias=True)
                document["_id"] = str(document["_id"])
                await persistent._db[TaskModel.collection_name()].insert_one(document)
                return task_id

            mock_submit.side_effect = submit

            with (
                patch("featurebyte.app.get_celery") as mock_get_celery,
                mock.patch("featurebyte.worker.task_executor.get_celery") as mock_get_celery_worker,
            ):

                def get_task(task_id):
                    status = task_status.get(task_id)
                    if status is None:
                        return None
                    return Mock(status=status)

                mock_get_celery.return_value.AsyncResult.side_effect = get_task
                mock_get_celery_worker.return_value.AsyncResult.side_effect = get_task
                yield


@pytest.fixture(scope="session")
def user():
    """
    Mock user
    """
    return User(id=None)


@pytest.fixture()
def online_store_table_version_service(app_container):
    """
    Fixture for online store table version service
    """
    yield app_container.online_store_table_version_service


@pytest.fixture()
def online_store_table_version_service_factory(mongo_database_name, app_container):
    """
    Fixture for a callback that returns a new OnlineStoreTableVersionService with a new persistent

    This is needed in tests where we need new instances of the persistent for different threads
    (the persistent object is not threadsafe)
    """

    def factory():
        app_container.override_instance_for_test(
            "persistent", get_new_persistent(mongo_database_name)[0]
        )
        return app_container.get(OnlineStoreTableVersionService)

    return factory


@pytest.fixture(name="task_manager")
def task_manager_fixture(persistent, user, catalog, storage):
    """
    Return a task manager used in tests.
    """
    task_manager = TaskManager(
        user=user,
        persistent=persistent,
        celery=get_celery(),
        catalog_id=catalog.id,
        storage=storage,
        redis=redis.from_url(TEST_REDIS_URI),
    )
    return task_manager


@pytest.fixture(name="app_container")
def app_container_fixture(persistent, user, catalog, storage):
    """
    Return an app container used in tests. This will allow us to easily retrieve instances of the right type.
    """
    instance_map = {
        "user": user,
        "persistent": persistent,
        "celery": get_celery(),
        "storage": storage,
        "catalog_id": catalog.id,
        "redis": redis.from_url(TEST_REDIS_URI),
    }
    return LazyAppContainer(app_container_config=app_container_config, instance_map=instance_map)


@pytest.fixture(name="app_container_no_catalog")
def app_container_no_catalog_fixture(persistent, user, storage):
    """
    Return an app container used in tests. This will allow us to easily retrieve instances of the right type.
    """
    instance_map = {
        "user": user,
        "persistent": persistent,
        "celery": get_celery(),
        "storage": storage,
        "catalog_id": DEFAULT_CATALOG_ID,
        "redis": redis.from_url(TEST_REDIS_URI),
    }
    return LazyAppContainer(app_container_config=app_container_config, instance_map=instance_map)


@pytest.fixture(name="feature_manager_service")
def feature_manager_service_fixture(app_container):
    """
    Return a feature manager service used in tests.
    """
    return app_container.feature_manager_service


@pytest.fixture(name="tile_scheduler_service")
def tile_scheduler_service_fixture(app_container):
    """
    Fixture for TileSchedulerService
    """
    return app_container.tile_scheduler_service


@pytest.fixture(name="tile_registry_service")
def tile_registry_service_fixture(app_container):
    """
    Fixture for TileRegistryService
    """
    return app_container.tile_registry_service


@pytest.fixture(name="online_store_compute_query_service")
def online_store_compute_query_service_fixture(app_container) -> OnlineStoreComputeQueryService:
    """
    Online store compute query service fixture
    """
    return app_container.online_store_compute_query_service


@pytest.fixture(name="feature_store_service")
def feature_store_service_fixture(app_container):
    """FeatureStore service"""
    return app_container.feature_store_service


@pytest.fixture(name="feature_table_cache_service")
def feature_table_cache_service_fixture(app_container):
    """FeatureTableCacheService fixture"""
    return app_container.feature_table_cache_service


@pytest.fixture(name="observation_table_service")
def observation_table_service_fixture(app_container):
    """ObservationTableService fixture"""
    return app_container.observation_table_service


@pytest.fixture(name="feature_list_service")
def feature_list_service_fixture(app_container):
    """FeatureListService fixture"""
    return app_container.feature_list_service


@pytest.fixture(name="feature_table_cache_metadata_service")
def feature_table_cache_metadata_service_fixture(app_container):
    """FeatureTableCacheMetadataService fixture"""
    return app_container.feature_table_cache_metadata_service


@pytest.fixture(name="feature_service")
def feature_service_fixture(app_container):
    """FeatureService fixture"""
    return app_container.feature_service


@pytest.fixture(name="online_store", scope="session")
def online_store_fixture():
    """
    Fixture for an OnlineStore
    """
    return OnlineStore.create(
        name="My Online Store",
        details=RedisOnlineStoreDetails(
            redis_type="redis",
            connection_string=TEST_REDIS_URI.replace("redis://", ""),
        ),
    )


@pytest.fixture(name="feature_group")
def feature_group_fixture(event_view):
    """
    Fixture for a simple FeatureGroup with count features
    """
    event_view["derived_value_column"] = 1.0 * event_view["ÜSER ID"]
    feature_group = event_view.groupby("ÜSER ID").aggregate_over(
        method="count",
        windows=["2h", "24h"],
        feature_names=["COUNT_2h", "COUNT_24h"],
    )
    return feature_group


@pytest.fixture(name="feature_group_per_category")
def feature_group_per_category_fixture(event_view):
    """
    Fixture for a FeatureGroup with dictionary features
    """

    feature_group_per_category = event_view.groupby(
        "ÜSER ID", category="PRODUCT_ACTION"
    ).aggregate_over(
        method="count",
        windows=["2h", "24h"],
        feature_names=["COUNT_BY_ACTION_2h", "COUNT_BY_ACTION_24h"],
    )
    # add features based on transformations on count per category
    feature_counts_24h = feature_group_per_category["COUNT_BY_ACTION_24h"]
    feature_group_per_category["ENTROPY_BY_ACTION_24h"] = feature_counts_24h.cd.entropy()
    feature_group_per_category["MOST_FREQUENT_ACTION_24h"] = feature_counts_24h.cd.most_frequent()
    feature_group_per_category["NUM_UNIQUE_ACTION_24h"] = feature_counts_24h.cd.unique_count()
    feature_group_per_category["NUM_UNIQUE_ACTION_24h_exclude_missing"] = (
        feature_counts_24h.cd.unique_count(include_missing=False)
    )

    feature_counts_2h = feature_group_per_category["COUNT_BY_ACTION_2h"]
    feature_group_per_category["ACTION_SIMILARITY_2h_to_24h"] = (
        feature_counts_2h.cd.cosine_similarity(feature_counts_24h)
    )

    return feature_group_per_category


@pytest.fixture(name="count_distinct_feature_group")
def count_distinct_feature_group_fixture(item_table):
    """
    Count distinct feature group fixture
    """
    item_view = item_table.get_view()
    item_count_by_cust = item_view.groupby("CUST_ID").aggregate_over(
        value_column=None, method="count", feature_names=["cust_count_of_items_4h"], windows=["4h"]
    )["cust_count_of_items_4h"]
    item_count_distinct_by_cust = item_view.groupby("CUST_ID").aggregate_over(
        value_column="item_id",
        method="count_distinct",
        feature_names=["cust_count_distinct_items_4h"],
        windows=["4h"],
    )["cust_count_distinct_items_4h"]
    cust_avg_count_of_items_per_type = item_count_by_cust / item_count_distinct_by_cust
    cust_avg_count_of_items_per_type.name = "cust_avg_count_of_items_per_type_4h"
    return FeatureGroup(
        [item_count_by_cust, item_count_distinct_by_cust, cust_avg_count_of_items_per_type]
    )


@pytest.fixture(name="mock_graph_clear_period", autouse=True)
def mock_graph_clear_period_fixture():
    """
    Mock graph clear period
    """
    with patch.dict(os.environ, {"FEATUREBYTE_GRAPH_CLEAR_PERIOD": "1000"}):
        # mock graph clear period to high value to clearing graph in tests
        # clearing graph in tests will cause test failures as the task & client sharing the same process space
        yield
