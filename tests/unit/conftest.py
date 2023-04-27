# pylint: disable=too-many-lines
"""
Common test fixtures used across unit test directories
"""
import json
import tempfile
import traceback
from contextlib import contextmanager
from datetime import datetime
from unittest import mock
from unittest.mock import Mock, PropertyMock, patch
from uuid import uuid4

import pandas as pd
import pytest
import pytest_asyncio
from bson.objectid import ObjectId
from cachetools import TTLCache
from fastapi.testclient import TestClient
from snowflake.connector.constants import QueryStatus

from featurebyte import (
    FeatureJobSetting,
    MissingValueImputation,
    SnowflakeDetails,
    UsernamePasswordCredential,
)
from featurebyte.api.api_object import ApiObject
from featurebyte.api.entity import Entity
from featurebyte.api.feature import DefaultVersionMode, Feature
from featurebyte.api.feature_group import FeatureGroup
from featurebyte.api.feature_list import FeatureList
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.groupby import GroupBy
from featurebyte.api.item_table import ItemTable
from featurebyte.app import User, app
from featurebyte.common.model_util import get_version
from featurebyte.enum import AggFunc, DBVarType, InternalName
from featurebyte.feature_manager.manager import FeatureManager
from featurebyte.feature_manager.model import ExtendedFeatureListModel
from featurebyte.models.base import DEFAULT_CATALOG_ID, VersionIdentifier
from featurebyte.models.credential import CredentialModel
from featurebyte.models.feature import FeatureReadiness
from featurebyte.models.feature_list import FeatureListNamespaceModel, FeatureListStatus
from featurebyte.models.relationship import RelationshipType
from featurebyte.models.request_input import SourceTableRequestInput
from featurebyte.models.task import Task as TaskModel
from featurebyte.models.tile import TileSpec
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.routes.app_container import AppContainer
from featurebyte.schema.batch_feature_table import BatchFeatureTableCreate
from featurebyte.schema.batch_request_table import BatchRequestTableCreate
from featurebyte.schema.context import ContextCreate
from featurebyte.schema.deployment import DeploymentCreate
from featurebyte.schema.feature_job_setting_analysis import FeatureJobSettingAnalysisCreate
from featurebyte.schema.feature_list import FeatureListGetHistoricalFeatures
from featurebyte.schema.feature_namespace import FeatureNamespaceCreate
from featurebyte.schema.historical_feature_table import HistoricalFeatureTableCreate
from featurebyte.schema.observation_table import ObservationTableCreate
from featurebyte.schema.relationship_info import RelationshipInfoCreate
from featurebyte.schema.task import TaskStatus
from featurebyte.schema.worker.task.base import BaseTaskPayload
from featurebyte.service.task_manager import TaskManager
from featurebyte.session.base import DEFAULT_EXECUTE_QUERY_TIMEOUT_SECONDS
from featurebyte.session.manager import SessionManager, session_cache
from featurebyte.storage import LocalTempStorage
from featurebyte.storage.local import LocalStorage
from featurebyte.tile.manager import TileManager
from featurebyte.worker.task.base import TASK_MAP
from tests.unit.conftest_config import (
    config_file_fixture,
    config_fixture,
    mock_config_path_env_fixture,
)
from tests.util.helper import iet_entropy

# register tests.unit.routes.base so that API stacktrace display properly
pytest.register_assert_rewrite("tests.unit.routes.base")

# "Registering" fixtures so that they'll be available for use as if they were defined here.
# We keep the definition in a separate file for readability
_ = [config_file_fixture, config_fixture, mock_config_path_env_fixture]


@pytest.fixture(name="mock_api_object_cache")
def mock_api_object_cache_fixture():
    """Mock api object cache so that the time-to-live period is 0"""
    with patch.object(ApiObject, "_cache", new_callable=PropertyMock) as mock_cache:
        mock_cache.return_value = TTLCache(maxsize=1024, ttl=0)
        yield


@pytest.fixture(autouse=True)
def mock_api_client_fixture():
    """
    Mock Configurations.get_client to use test client
    """
    with mock.patch("featurebyte.config.BaseAPIClient.request") as mock_request:
        with TestClient(app) as client:
            mock_request.side_effect = client.request
            yield mock_request


@pytest.fixture(autouse=True)
def mock_websocket_client_fixture():
    """
    Mock Configurations.get_websocket_client to use test client
    """
    with mock.patch(
        "featurebyte.config.Configurations.get_websocket_client"
    ) as mock_get_websocket_client:
        mock_get_websocket_client.return_value.__enter__.return_value.receive_json.return_value = (
            None
        )
        yield mock_get_websocket_client


@pytest.fixture(name="storage")
def storage_fixture():
    """
    Storage object fixture
    """
    with tempfile.TemporaryDirectory() as tempdir:
        yield LocalStorage(base_path=tempdir)


@pytest.fixture(name="temp_storage")
def temp_storage_fixture():
    """
    Storage object fixture
    """
    with tempfile.TemporaryDirectory() as tempdir:
        yield LocalStorage(base_path=tempdir)


@pytest.fixture(name="mock_get_persistent")
def mock_get_persistent_function(mongo_persistent):
    """
    Mock get_persistent in featurebyte.app
    """
    with mock.patch("featurebyte.app.get_persistent") as mock_persistent:
        persistent, _ = mongo_persistent
        mock_persistent.return_value = persistent
        yield mock_persistent


@pytest.fixture(autouse=True)
def mock_settings_env_vars(mock_config_path_env, mock_get_persistent):
    """Use these fixtures for all tests"""
    _ = mock_config_path_env, mock_get_persistent
    yield


@pytest.fixture(name="snowflake_connector")
def mock_snowflake_connector():
    """
    Mock snowflake connector in featurebyte.session.snowflake module
    """
    with mock.patch("featurebyte.session.snowflake.connector") as mock_connector:
        connection = mock_connector.connect.return_value
        connection.get_query_status_throw_if_error.return_value = QueryStatus.SUCCESS
        connection.is_still_running.return_value = False
        cursor = connection.cursor.return_value
        cursor.sfqid = "some-query-id"
        cursor.fetch_arrow_batches.return_value = []
        yield mock_connector


@pytest.fixture(name="snowflake_query_map")
def snowflake_query_map_fixture():
    """snowflake query map fixture"""
    query_map = {
        "SHOW DATABASES": [{"name": "sf_database"}],
        'SHOW SCHEMAS IN DATABASE "sf_database"': [{"name": "sf_schema"}],
        'SHOW TABLES IN SCHEMA "sf_database"."sf_schema"': [{"name": "sf_table"}],
        'SHOW VIEWS IN SCHEMA "sf_database"."sf_schema"': [{"name": "sf_view"}],
        'SHOW COLUMNS IN "sf_database"."sf_schema"."sf_table"': [
            {"column_name": "col_int", "data_type": json.dumps({"type": "FIXED", "scale": 0})},
            {"column_name": "col_float", "data_type": json.dumps({"type": "REAL"})},
            {
                "column_name": "col_char",
                "data_type": json.dumps({"type": "TEXT", "length": 1}),
            },
            {
                "column_name": "col_text",
                "data_type": json.dumps({"type": "TEXT", "length": 2**24}),
            },
            {"column_name": "col_binary", "data_type": json.dumps({"type": "BINARY"})},
            {"column_name": "col_boolean", "data_type": json.dumps({"type": "BOOLEAN"})},
            {
                "column_name": "event_timestamp",
                "data_type": json.dumps({"type": "TIMESTAMP_TZ"}),
            },
            {"column_name": "created_at", "data_type": json.dumps({"type": "TIMESTAMP_TZ"})},
            {"column_name": "cust_id", "data_type": json.dumps({"type": "FIXED", "scale": 0})},
        ],
        'SHOW COLUMNS IN "sf_database"."sf_schema"."sf_table_no_tz"': [
            {
                "column_name": "event_timestamp",
                "data_type": json.dumps({"type": "TIMESTAMP_NTZ"}),
            },
            {"column_name": "created_at", "data_type": json.dumps({"type": "TIMESTAMP_NTZ"})},
            {"column_name": "col_int", "data_type": json.dumps({"type": "FIXED", "scale": 0})},
            {"column_name": "cust_id", "data_type": json.dumps({"type": "FIXED", "scale": 0})},
            {
                "column_name": "tz_offset",
                "data_type": json.dumps({"type": "TEXT", "length": 2**24}),
            },
        ],
        'SHOW COLUMNS IN "sf_database"."sf_schema"."sf_view"': [
            {"column_name": "col_date", "data_type": json.dumps({"type": "DATE"})},
            {"column_name": "col_time", "data_type": json.dumps({"type": "TIME"})},
            {
                "column_name": "col_timestamp_ltz",
                "data_type": json.dumps({"type": "TIMESTAMP_LTZ"}),
            },
            {
                "column_name": "col_timestamp_ntz",
                "data_type": json.dumps({"type": "TIMESTAMP_NTZ"}),
            },
            {
                "column_name": "col_timestamp_tz",
                "data_type": json.dumps({"type": "TIMESTAMP_TZ"}),
            },
        ],
        'SHOW COLUMNS IN "sf_database"."sf_schema"."items_table"': [
            {
                "column_name": "event_id_col",
                "data_type": json.dumps({"type": "FIXED", "scale": 0}),
            },
            {
                "column_name": "item_id_col",
                "data_type": json.dumps({"type": "TEXT", "length": 2**24}),
            },
            {
                "column_name": "item_type",
                "data_type": json.dumps({"type": "TEXT", "length": 2**24}),
            },
            {"column_name": "item_amount", "data_type": json.dumps({"type": "REAL"})},
            {"column_name": "created_at", "data_type": json.dumps({"type": "TIMESTAMP_TZ"})},
            {
                "column_name": "event_timestamp",
                "data_type": json.dumps({"type": "TIMESTAMP_TZ"}),
            },
        ],
        'SHOW COLUMNS IN "sf_database"."sf_schema"."items_table_same_event_id"': [
            {
                "column_name": "col_int",
                "data_type": json.dumps({"type": "FIXED", "scale": 0}),
            },
            {
                "column_name": "item_id_col",
                "data_type": json.dumps({"type": "TEXT", "length": 2**24}),
            },
            {"column_name": "created_at", "data_type": json.dumps({"type": "TIMESTAMP_TZ"})},
        ],
        'SHOW COLUMNS IN "sf_database"."sf_schema"."fixed_table"': [
            {"column_name": "num", "data_type": json.dumps({"type": "FIXED", "scale": 0})},
            {"column_name": "num10", "data_type": json.dumps({"type": "FIXED", "scale": 1})},
            {"column_name": "dec", "data_type": json.dumps({"type": "FIXED", "scale": 2})},
        ],
        'SHOW COLUMNS IN "sf_database"."sf_schema"."non_scalar_table"': [
            {
                "column_name": "variant",
                "data_type": json.dumps({"type": "VARIANT", "nullable": True}),
            },
        ],
        'SHOW COLUMNS IN "sf_database"."sf_schema"."scd_table"': [
            {"column_name": "col_int", "data_type": json.dumps({"type": "FIXED", "scale": 0})},
            {"column_name": "col_float", "data_type": json.dumps({"type": "REAL"})},
            {
                "column_name": "is_active",
                "data_type": json.dumps({"type": "BOOLEAN", "length": 1}),
            },
            {
                "column_name": "col_text",
                "data_type": json.dumps({"type": "TEXT", "length": 2**24}),
            },
            {"column_name": "col_binary", "data_type": json.dumps({"type": "BINARY"})},
            {"column_name": "col_boolean", "data_type": json.dumps({"type": "BOOLEAN"})},
            {
                "column_name": "effective_timestamp",
                "data_type": json.dumps({"type": "TIMESTAMP_TZ"}),
            },
            {
                "column_name": "end_timestamp",
                "data_type": json.dumps({"type": "TIMESTAMP_TZ"}),
            },
            {"column_name": "created_at", "data_type": json.dumps({"type": "TIMESTAMP_TZ"})},
            {"column_name": "cust_id", "data_type": json.dumps({"type": "FIXED", "scale": 0})},
        ],
        "SHOW SCHEMAS": [
            {"name": "PUBLIC"},
        ],
    }
    query_map['SHOW COLUMNS IN "sf_database"."sf_schema"."dimension_table"'] = query_map[
        'SHOW COLUMNS IN "sf_database"."sf_schema"."sf_table"'
    ]
    return query_map


@pytest.fixture(name="snowflake_execute_query")
def mock_snowflake_execute_query(snowflake_connector, snowflake_query_map):
    """
    Mock execute_query in featurebyte.session.snowflake.SnowflakeSession class
    """
    _ = snowflake_connector

    def side_effect(query, timeout=DEFAULT_EXECUTE_QUERY_TIMEOUT_SECONDS):
        _ = timeout
        res = snowflake_query_map.get(query)
        if res is not None:
            return pd.DataFrame(res)
        return None

    with mock.patch(
        "featurebyte.session.snowflake.SnowflakeSession.execute_query"
    ) as mock_execute_query:
        mock_execute_query.side_effect = side_effect
        yield mock_execute_query


@pytest.fixture(name="snowflake_feature_store_params")
def snowflake_feature_store_params_fixture():
    """
    Snowflake database source params fixture
    """
    return {
        "name": "sf_featurestore",
        "source_type": "snowflake",
        "details": SnowflakeDetails(
            account="sf_account",
            warehouse="sf_warehouse",
            sf_schema="sf_schema",
            database="sf_database",
        ),
        "database_credential": UsernamePasswordCredential(
            username="sf_user",
            password="sf_password",
        ),
    }


@pytest.fixture(name="snowflake_feature_store")
def snowflake_feature_store_fixture(snowflake_feature_store_params, snowflake_execute_query):
    """
    Snowflake database source fixture
    """
    _ = snowflake_execute_query
    return FeatureStore.create(**snowflake_feature_store_params)


@pytest.fixture(name="credentials")
def credentials_fixture(snowflake_feature_store_params):
    """
    Credentials fixture
    """
    return {
        snowflake_feature_store_params["name"]: CredentialModel(
            name="sf_featurestore",
            feature_store_id=ObjectId(),
            database_credential=UsernamePasswordCredential(
                username="sf_user",
                password="sf_password",
            ),
        )
    }


@pytest.fixture(name="snowflake_data_source")
def snowflake_data_source_fixture(snowflake_feature_store):
    """
    Snowflake table source fixture
    """
    return snowflake_feature_store.get_data_source()


@pytest.fixture(name="snowflake_database_table")
def snowflake_database_table_fixture(
    snowflake_data_source,
):
    """
    SourceTable object fixture
    """
    snowflake_table = snowflake_data_source.get_source_table(
        database_name="sf_database",
        schema_name="sf_schema",
        table_name="sf_table",
    )
    assert isinstance(snowflake_table.feature_store, FeatureStore)
    yield snowflake_table


@pytest.fixture(name="snowflake_database_table_no_tz")
def snowflake_database_table_no_tz_fixture(
    snowflake_data_source,
):
    """
    SourceTable object fixture where timestamp columns have no timezone
    """
    snowflake_table = snowflake_data_source.get_source_table(
        database_name="sf_database",
        schema_name="sf_schema",
        table_name="sf_table_no_tz",
    )
    assert isinstance(snowflake_table.feature_store, FeatureStore)
    yield snowflake_table


@pytest.fixture(name="snowflake_database_table_item_table")
def snowflake_database_table_item_table_fixture(snowflake_data_source):
    """
    SourceTable object fixture for ItemTable
    """
    yield snowflake_data_source.get_source_table(
        database_name="sf_database",
        schema_name="sf_schema",
        table_name="items_table",
    )


@pytest.fixture(name="snowflake_database_table_scd_table")
def snowflake_database_table_scd_table_fixture(snowflake_data_source):
    """
    SourceTable object fixture for SCDTable
    """
    yield snowflake_data_source.get_source_table(
        database_name="sf_database",
        schema_name="sf_schema",
        table_name="scd_table",
    )


@pytest.fixture(name="snowflake_database_table_dimension_table")
def snowflake_database_table_dimension_table_fixture(snowflake_data_source):
    """
    SourceTable object fixture for DimensionTable
    """
    yield snowflake_data_source.get_source_table(
        database_name="sf_database",
        schema_name="sf_schema",
        table_name="dimension_table",
    )


@pytest.fixture(name="snowflake_database_table_item_table_same_event_id")
def snowflake_database_table_item_table_same_event_id_fixture(snowflake_data_source):
    """
    SourceTable object fixture for ItemTable (same event_id_column with EventTable)
    """
    yield snowflake_data_source.get_source_table(
        database_name="sf_database",
        schema_name="sf_schema",
        table_name="items_table_same_event_id",
    )


@pytest.fixture(name="snowflake_dimension_table_id")
def snowflake_dimension_table_id_fixture():
    """Snowflake dimension table ID"""
    return ObjectId("6337f9651050ee7d1234660d")


@pytest.fixture(name="snowflake_scd_table_id")
def snowflake_scd_table_id_fixture():
    """Snowflake SCD table ID"""
    return ObjectId("6337f9651050ee7d123466cd")


@pytest.fixture(name="snowflake_event_table_id")
def snowflake_event_table_id_fixture():
    """Snowflake event table ID"""
    return ObjectId("6337f9651050ee7d5980660d")


@pytest.fixture(name="snowflake_event_table_with_tz_offset_column_id")
def snowflake_event_table_with_tz_offset_column_id_fixture():
    """Snowflake event table ID"""
    return ObjectId("64468d2ea444e44a0df168d8")


@pytest.fixture(name="snowflake_event_table_with_tz_offset_constant_id")
def snowflake_event_table_with_tz_offset_constant_id_fixture():
    """Snowflake event table ID"""
    return ObjectId("64468d44a444e44a0df168d9")


@pytest.fixture(name="snowflake_item_table_id")
def snowflake_item_table_id_fixture():
    """Snowflake event table ID"""
    return ObjectId("6337f9651050ee7d5980662d")


@pytest.fixture(name="snowflake_item_table_id_2")
def snowflake_item_table_id_2_fixture():
    """Snowflake event table ID"""
    return ObjectId("6337f9651050ee7d5980662e")


@pytest.fixture(name="cust_id_entity_id")
def cust_id_entity_id_fixture():
    """Customer ID entity ID"""
    # Note that these IDs are part of the groupby node parameters, it will affect the node hash calculation.
    # Altering these IDs may cause the SDK code generation to fail (due to the generated code could slightly
    # be different).
    return ObjectId("63f94ed6ea1f050131379214")


@pytest.fixture(name="transaction_entity_id")
def transaction_entity_id_fixture():
    """Transaction entity ID"""
    # Note that these IDs are part of the groupby node parameters, it will affect the node hash calculation.
    # Altering these IDs may cause the SDK code generation to fail (due to the generated code could slightly
    # be different).
    return ObjectId("63f94ed6ea1f050131379204")


@pytest.fixture(name="snowflake_event_table")
def snowflake_event_table_fixture(snowflake_database_table, snowflake_event_table_id):
    """EventTable object fixture"""
    event_table = snowflake_database_table.create_event_table(
        name="sf_event_table",
        event_id_column="col_int",
        event_timestamp_column="event_timestamp",
        record_creation_timestamp_column="created_at",
        _id=snowflake_event_table_id,
    )
    assert event_table.frame.node.parameters.id == event_table.id
    yield event_table


@pytest.fixture(name="snowflake_event_table_with_tz_offset_column")
def snowflake_event_table_with_tz_offset_column_fixture(
    snowflake_database_table_no_tz,
    snowflake_event_table_with_tz_offset_column_id,
    transaction_entity,
    cust_id_entity,
):
    """EventTable object fixture"""
    event_table = snowflake_database_table_no_tz.create_event_table(
        name="sf_event_table",
        event_id_column="col_int",
        event_timestamp_column="event_timestamp",
        event_timestamp_timezone_offset_column="tz_offset",
        record_creation_timestamp_column="created_at",
        _id=snowflake_event_table_with_tz_offset_column_id,
    )
    event_table["col_int"].as_entity(transaction_entity.name)
    event_table["cust_id"].as_entity(cust_id_entity.name)
    yield event_table


@pytest.fixture(name="snowflake_event_table_with_tz_offset_constant")
def snowflake_event_table_with_tz_offset_constant_fixture(
    snowflake_database_table_no_tz,
    snowflake_event_table_with_tz_offset_constant_id,
    transaction_entity,
    cust_id_entity,
):
    """EventTable object fixture"""
    event_table = snowflake_database_table_no_tz.create_event_table(
        name="sf_event_table",
        event_id_column="col_int",
        event_timestamp_column="event_timestamp",
        event_timestamp_timezone_offset="-05:30",
        record_creation_timestamp_column="created_at",
        _id=snowflake_event_table_with_tz_offset_constant_id,
    )
    event_table["col_int"].as_entity(transaction_entity.name)
    event_table["cust_id"].as_entity(cust_id_entity.name)
    yield event_table


@pytest.fixture(name="snowflake_dimension_table")
def snowflake_dimension_table_fixture(
    snowflake_database_table_dimension_table, snowflake_dimension_table_id
):
    """DimensionTable object fixture"""
    dimension_table = snowflake_database_table_dimension_table.create_dimension_table(
        name="sf_dimension_table",
        dimension_id_column="col_int",
        record_creation_timestamp_column="created_at",
        _id=snowflake_dimension_table_id,
    )
    assert dimension_table.frame.node.parameters.id == dimension_table.id
    yield dimension_table


@pytest.fixture(name="snowflake_scd_table")
def snowflake_scd_table_fixture(snowflake_database_table_scd_table, snowflake_scd_table_id):
    """SCDTable object fixture"""
    scd_table = snowflake_database_table_scd_table.create_scd_table(
        name="sf_scd_table",
        natural_key_column="col_text",
        surrogate_key_column="col_int",
        effective_timestamp_column="effective_timestamp",
        end_timestamp_column="end_timestamp",
        current_flag_column="is_active",
        _id=snowflake_scd_table_id,
    )
    assert scd_table.frame.node.parameters.id == scd_table.id
    yield scd_table


@pytest.fixture(name="snowflake_scd_table_with_entity")
def snowflake_scd_table_with_entity_fixture(snowflake_scd_table, cust_id_entity):
    """
    Fixture for an SCD table with entity
    """
    snowflake_scd_table["col_text"].as_entity(cust_id_entity.name)
    return snowflake_scd_table


@pytest.fixture(name="snowflake_item_table")
def snowflake_item_table_fixture(
    snowflake_database_table_item_table,
    mock_get_persistent,
    snowflake_item_table_id,
    snowflake_event_table,
):
    """
    Snowflake ItemTable object fixture
    """
    _ = mock_get_persistent
    item_table = snowflake_database_table_item_table.create_item_table(
        name="sf_item_table",
        event_id_column="event_id_col",
        item_id_column="item_id_col",
        event_table_name=snowflake_event_table.name,
        _id=snowflake_item_table_id,
    )
    assert item_table.frame.node.parameters.id == item_table.id
    yield item_table


@pytest.fixture(name="snowflake_item_table_same_event_id")
def snowflake_item_table_same_event_id_fixture(
    snowflake_database_table_item_table_same_event_id,
    mock_get_persistent,
    snowflake_item_table_id_2,
    snowflake_event_table,
    snowflake_item_table,
):
    """
    Snowflake ItemTable object fixture (same event_id_column as EventTable)
    """
    _ = mock_get_persistent
    _ = snowflake_item_table
    event_id_column = "col_int"
    assert snowflake_event_table.event_id_column == event_id_column
    yield snowflake_database_table_item_table_same_event_id.create_item_table(
        name="sf_item_table_2",
        event_id_column=event_id_column,
        item_id_column="item_id_col",
        event_table_name=snowflake_event_table.name,
        _id=snowflake_item_table_id_2,
    )


@pytest.fixture(name="snowflake_item_table_with_timezone_offset_column")
def snowflake_item_table_with_timezone_offset_column_fixture(
    snowflake_database_table_item_table,
    mock_get_persistent,
    snowflake_event_table_with_tz_offset_column,
):
    """
    Snowflake ItemTable object fixture where the EventTable has a timezone offset column
    """
    _ = mock_get_persistent
    item_table = snowflake_database_table_item_table.create_item_table(
        name="sf_item_table",
        event_id_column="event_id_col",
        item_id_column="item_id_col",
        event_table_name=snowflake_event_table_with_tz_offset_column.name,
    )
    yield item_table


@pytest.fixture(name="cust_id_entity")
def cust_id_entity_fixture(cust_id_entity_id):
    """
    Customer ID entity fixture
    """
    entity = Entity(name="customer", serving_names=["cust_id"], _id=cust_id_entity_id)
    entity.save()
    yield entity


@pytest.fixture(name="transaction_entity")
def transaction_entity_fixture(transaction_entity_id):
    """
    Event entity fixture
    """
    entity = Entity(name="transaction", serving_names=["transaction_id"], _id=transaction_entity_id)
    entity.save()
    assert entity.id == transaction_entity_id
    yield entity


@pytest.fixture(name="snowflake_event_table_with_entity")
def snowflake_event_table_with_entity_fixture(
    snowflake_event_table, cust_id_entity, transaction_entity, mock_api_object_cache
):
    """
    Entity fixture that sets cust_id in snowflake_event_table as an Entity
    """
    _ = mock_api_object_cache
    snowflake_event_table.cust_id.as_entity(cust_id_entity.name)
    snowflake_event_table.col_int.as_entity(transaction_entity.name)
    yield snowflake_event_table


@pytest.fixture(name="arbitrary_default_feature_job_setting")
def arbitrary_default_feature_job_setting_fixture():
    """
    Get arbitrary default feature job setting
    """
    return FeatureJobSetting(blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m")


@pytest.fixture(name="snowflake_event_table_with_entity_and_feature_job")
def snowflake_event_table_with_entity_and_feature_job_fixture(
    snowflake_event_table, cust_id_entity, arbitrary_default_feature_job_setting
):
    """
    Entity fixture that sets cust_id in snowflake_event_table as an Entity
    """
    snowflake_event_table.cust_id.as_entity(cust_id_entity.name)
    snowflake_event_table.update_default_feature_job_setting(
        feature_job_setting=arbitrary_default_feature_job_setting
    )
    yield snowflake_event_table


@pytest.fixture(name="snowflake_feature_store_details_dict")
def snowflake_feature_store_details_dict_fixture():
    """Feature store details dict fixture"""
    return {
        "type": "snowflake",
        "details": {
            "account": "sf_account",
            "database": "sf_database",
            "sf_schema": "sf_schema",
            "warehouse": "sf_warehouse",
        },
    }


@pytest.fixture(name="snowflake_table_details_dict")
def snowflake_table_details_dict_fixture():
    """Table details dict fixture"""
    return {
        "database_name": "sf_database",
        "schema_name": "sf_schema",
        "table_name": "sf_table",
    }


@pytest.fixture(name="snowflake_event_view_with_entity")
def snowflake_event_view_entity_fixture(snowflake_event_table_with_entity):
    """
    Snowflake event view with entity
    """
    event_view = snowflake_event_table_with_entity.get_view()
    yield event_view


@pytest.fixture(name="snowflake_event_view_with_entity_and_feature_job")
def snowflake_event_view_entity_feature_job_fixture(
    snowflake_event_table_with_entity_and_feature_job,
):
    """
    Snowflake event view with entity
    """
    event_view = snowflake_event_table_with_entity_and_feature_job.get_view()
    yield event_view


@pytest.fixture(name="patched_observation_table_service")
def patched_observation_table_service():
    """
    Patch ObservationTableService.validate_materialized_table_and_get_metadata
    """

    async def mocked_get_additional_metadata(*args, **kwargs):
        _ = args
        _ = kwargs
        return {
            "columns_info": [
                {"name": "POINT_IN_TIME", "dtype": "TIMESTAMP"},
                {"name": "cust_id", "dtype": "INT"},
            ],
            "most_recent_point_in_time": "2023-01-15 10:00:00",
        }

    with patch(
        "featurebyte.service.observation_table.ObservationTableService.validate_materialized_table_and_get_metadata",
        Mock(side_effect=mocked_get_additional_metadata),
    ):
        yield


@pytest.fixture(name="snowflake_execute_query_batch_request_table_patcher")
def snowflake_execute_query_batch_reqeust_table_patcher():
    """Fixture to patch SnowflakeSession.execute_query to return mock data for batch request table"""

    @contextmanager
    def patch_snowflake_execute_query(query_map, handle_batch_request_table_query):
        """Patch SnowflakeSession.execute_query to return mock data"""

        def side_effect(query, timeout=DEFAULT_EXECUTE_QUERY_TIMEOUT_SECONDS):
            _ = timeout
            if handle_batch_request_table_query and query.startswith(
                'SHOW COLUMNS IN "sf_database"."sf_schema"."BATCH_REQUEST_TABLE_'
            ):
                # return a cust_id column for batch request table to pass validation
                res = [
                    {
                        "column_name": "cust_id",
                        "data_type": json.dumps({"type": "FIXED", "scale": 0}),
                    }
                ]
            else:
                res = query_map.get(query)

            if res is not None:
                return pd.DataFrame(res)
            return None

        with mock.patch(
            "featurebyte.session.snowflake.SnowflakeSession.execute_query"
        ) as mock_execute_query:
            mock_execute_query.side_effect = side_effect
            yield mock_execute_query

    return patch_snowflake_execute_query


@pytest.fixture(name="observation_table_from_source")
def observation_table_from_source_fixture(
    snowflake_database_table, patched_observation_table_service
):
    """
    Observation table created from SourceTable
    """
    _ = patched_observation_table_service
    return snowflake_database_table.create_observation_table("observation_table_from_source_table")


@pytest.fixture(name="observation_table_from_view")
def observation_table_from_view_fixture(snowflake_event_view, patched_observation_table_service):
    """
    Observation table created from EventView
    """
    _ = patched_observation_table_service
    return snowflake_event_view.create_observation_table("observation_table_from_event_view")


@pytest.fixture(name="historical_feature_table")
def historical_feature_table_fixture(float_feature, observation_table_from_source):
    """
    Fixture for a HistoricalFeatureTable
    """
    feature_list = FeatureList([float_feature], name="feature_list_for_historical_feature_table")
    feature_list.save()
    historical_feature_table = feature_list.compute_historical_feature_table(
        observation_table_from_source, "my_historical_feature_table"
    )
    return historical_feature_table


@pytest.fixture(name="grouped_event_view")
def grouped_event_view_fixture(snowflake_event_view_with_entity):
    """
    EventViewGroupBy fixture
    """
    grouped = snowflake_event_view_with_entity.groupby("cust_id")
    assert isinstance(grouped, GroupBy)
    yield grouped


@pytest.fixture(name="feature_group_feature_job_setting")
def feature_group_feature_job_setting():
    """
    Get feature group feature job setting
    """
    return FeatureJobSetting(
        blind_spot="10m",
        frequency="30m",
        time_modulo_frequency="5m",
    )


@pytest.fixture(name="feature_group")
def feature_group_fixture(
    grouped_event_view,
    cust_id_entity,
    snowflake_event_table_with_entity,
    feature_group_feature_job_setting,
):
    """
    FeatureList fixture
    """
    snowflake_event_table_with_entity.update_default_feature_job_setting(
        feature_job_setting=feature_group_feature_job_setting,
    )
    global_graph = GlobalQueryGraph()
    assert id(global_graph.nodes) == id(grouped_event_view.view_obj.graph.nodes)
    feature_group = grouped_event_view.aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["30m", "2h", "1d"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=["sum_30m", "sum_2h", "sum_1d"],
    )
    assert isinstance(feature_group, FeatureGroup)
    for feature in feature_group.feature_objects.values():
        assert id(feature.graph.nodes) == id(global_graph.nodes)
        assert feature.table_ids == [snowflake_event_table_with_entity.id]
        assert feature.entity_ids == [cust_id_entity.id]
    yield feature_group


@pytest.fixture(name="production_ready_feature")
def production_ready_feature_fixture(feature_group):
    """Fixture for a production ready feature"""
    feature = feature_group["sum_30m"] + 123
    feature.name = "production_ready_feature"
    assert feature.parent is None
    feature.__dict__["readiness"] = FeatureReadiness.PRODUCTION_READY
    feature.__dict__["version"] = "V220401"
    feature_group["production_ready_feature"] = feature
    return feature


@pytest.fixture(name="feature_with_cleaning_operations")
def feature_with_cleaning_operations_fixture(
    snowflake_event_table, cust_id_entity, feature_group_feature_job_setting
):
    """
    Fixture to get a feature with cleaning operations
    """
    snowflake_event_table.cust_id.as_entity(cust_id_entity.name)
    snowflake_event_table["col_float"].update_critical_data_info(
        cleaning_operations=[
            MissingValueImputation(imputed_value=0.0),
        ]
    )
    event_view = snowflake_event_table.get_view()
    feature_group = event_view.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["30m"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=["sum_30m"],
    )
    yield feature_group["sum_30m"]


@pytest.fixture(name="non_time_based_feature")
def get_non_time_based_feature_fixture(snowflake_item_table, transaction_entity):
    """
    Get a non-time-based feature.

    This is a non-time-based feature as it is built from ItemTable.
    """
    snowflake_item_table.event_id_col.as_entity(transaction_entity.name)
    item_table = ItemTable(**{**snowflake_item_table.json_dict(), "item_id_column": "event_id_col"})
    item_view = item_table.get_view(event_suffix="_event_table")
    return item_view.groupby("event_id_col").aggregate(
        value_column="item_amount",
        method=AggFunc.SUM,
        feature_name="non_time_time_sum_amount_feature",
    )


@pytest.fixture(name="float_feature")
def float_feature_fixture(feature_group):
    """
    Float Feature fixture
    """
    feature = feature_group["sum_1d"]
    assert isinstance(feature, Feature)
    assert feature.protected_columns == {"cust_id"}
    assert feature.inherited_columns == {"cust_id"}
    assert feature_group["sum_1d"].table_ids == feature.table_ids
    global_graph = GlobalQueryGraph()
    assert id(feature.graph.nodes) == id(global_graph.nodes)
    yield feature


@pytest.fixture(name="bool_feature")
def bool_feature_fixture(float_feature):
    """
    Boolean Feature fixture
    """
    bool_feature = float_feature > 100.0
    assert isinstance(bool_feature, Feature)
    assert bool_feature.protected_columns == float_feature.protected_columns
    assert bool_feature.inherited_columns == float_feature.inherited_columns
    assert bool_feature.table_ids == float_feature.table_ids
    yield bool_feature


@pytest.fixture(name="agg_per_category_feature")
def agg_per_category_feature_fixture(snowflake_event_view_with_entity):
    """
    Aggregation per category feature fixture
    """
    grouped = snowflake_event_view_with_entity.groupby("cust_id", category="col_int")
    features = grouped.aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["30m", "2h", "1d"],
        feature_job_setting=FeatureJobSetting(
            blind_spot="10m",
            frequency="30m",
            time_modulo_frequency="5m",
        ),
        feature_names=["sum_30m", "sum_2h", "sum_1d"],
    )
    yield features["sum_1d"]


@pytest.fixture(name="count_per_category_feature_group")
def count_per_category_feature_group_fixture(snowflake_event_view_with_entity):
    """
    Aggregation per category FeatureGroup fixture
    """
    grouped = snowflake_event_view_with_entity.groupby("cust_id", category="col_int")
    features = grouped.aggregate_over(
        method="count",
        windows=["30m", "2h", "1d"],
        feature_job_setting=FeatureJobSetting(
            blind_spot="10m",
            frequency="30m",
            time_modulo_frequency="5m",
        ),
        feature_names=["counts_30m", "counts_2h", "counts_1d"],
    )
    yield features


@pytest.fixture(name="sum_per_category_feature")
def sum_per_category_feature_fixture(snowflake_event_view_with_entity):
    """
    Aggregation (sum) per category FeatureGroup fixture
    """
    grouped = snowflake_event_view_with_entity.groupby("cust_id", category="col_int")
    features = grouped.aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["30m"],
        feature_job_setting=FeatureJobSetting(
            blind_spot="10m",
            frequency="30m",
            time_modulo_frequency="5m",
        ),
        feature_names=["sum_30m"],
    )
    yield features["sum_30m"]


@pytest.fixture(name="count_per_category_feature")
def count_per_category_feature_fixture(count_per_category_feature_group):
    """
    Aggregation per category feature fixture (1d window)
    """
    yield count_per_category_feature_group["counts_1d"]


@pytest.fixture(name="count_per_category_feature_2h")
def count_per_category_feature_2h_fixture(count_per_category_feature_group):
    """
    Aggregation per category feature fixture (2h window)
    """
    yield count_per_category_feature_group["counts_2h"]


@pytest.fixture(name="session_manager")
def session_manager_fixture(credentials, snowflake_connector):
    """
    Session manager fixture
    """
    # pylint: disable=E1101
    _ = snowflake_connector
    session_cache.clear()
    yield SessionManager(credentials=credentials)


@pytest.fixture
def mock_snowflake_tile():
    """
    Pytest Fixture for TileSnowflake instance
    """

    tile_sql = (
        f"select c1 from dummy where"
        f" tile_start_ts >= {InternalName.TILE_START_DATE_SQL_PLACEHOLDER} and"
        f" tile_start_ts < {InternalName.TILE_END_DATE_SQL_PLACEHOLDER}"
    )
    tile_spec = TileSpec(
        time_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        tile_sql=tile_sql,
        tile_id="TILE_ID1",
        aggregation_id="agg_id1",
        value_column_names=["col2"],
        value_column_types=["FLOAT"],
        entity_column_names=["col1"],
    )

    return tile_spec


@pytest_asyncio.fixture
@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
async def tile_manager(mock_execute_query, session_manager, snowflake_feature_store):
    """
    Tile Manager fixture
    """
    _ = mock_execute_query
    return TileManager(session=await session_manager.get_session(snowflake_feature_store))


@pytest.fixture
@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
def mock_snowflake_feature(
    mock_execute_query, snowflake_connector, snowflake_event_view_with_entity
):
    """Fixture for a Feature object"""
    mock_execute_query.size_effect = None
    _ = snowflake_connector

    feature_group = snowflake_event_view_with_entity.groupby(by_keys="cust_id").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["30m"],
        feature_names=["sum_30m"],
        feature_job_setting=FeatureJobSetting(
            blind_spot="10m",
            frequency="30m",
            time_modulo_frequency="5m",
        ),
    )
    feature = feature_group["sum_30m"]
    feature.__dict__["online_enabled"] = False
    return feature


@pytest_asyncio.fixture
@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
async def feature_manager(mock_execute_query, session_manager, snowflake_feature_store):
    """
    Feature Manager fixture
    """
    _ = mock_execute_query
    return FeatureManager(session=await session_manager.get_session(snowflake_feature_store))


@pytest.fixture
@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
def mock_snowflake_feature_list_model(
    mock_execute_query, snowflake_connector, snowflake_event_view_with_entity
):
    """Fixture for a FeatureListModel"""
    mock_execute_query.size_effect = None
    _ = snowflake_connector

    feature_group = snowflake_event_view_with_entity.groupby(by_keys="cust_id").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["30m"],
        feature_names=["sum_30m"],
        feature_job_setting=FeatureJobSetting(
            blind_spot="10m",
            frequency="30m",
            time_modulo_frequency="5m",
        ),
    )
    feature = feature_group["sum_30m"]

    mock_feature_list = ExtendedFeatureListModel(
        name="feature_list1",
        feature_ids=[feature.id],
        feature_signatures=[
            {
                "id": feature.id,
                "name": feature.name,
                "version": VersionIdentifier(name=get_version()),
            }
        ],
        readiness=FeatureReadiness.DRAFT,
        status=FeatureListStatus.PUBLIC_DRAFT,
        version=VersionIdentifier(name="v1"),
    )

    return mock_feature_list


@pytest.fixture(name="mocked_compute_tiles_on_demand")
def mocked_compute_tiles_on_demand():
    """Fixture for a mocked SnowflakeTileCache object"""
    with mock.patch(
        "featurebyte.query_graph.sql.feature_historical.TileCache.compute_tiles_on_demand"
    ) as mocked_compute_tiles_on_demand:
        yield mocked_compute_tiles_on_demand


@pytest.fixture(name="noop_validate_feature_store_id_not_used_in_warehouse", autouse=True)
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


@pytest.fixture(name="noop_session_validator", autouse=True)
def get_noop_session_validator_fixture():
    """
    Set a no-op validator by default.

    Functions that want to test the validation should inject an actual instance of the session validator.
    """
    with mock.patch(
        "featurebyte.service.session_validator.SessionValidatorService.validate_feature_store_exists"
    ) as mocked_exists:
        mocked_exists.return_value = None
        yield


@pytest.fixture(name="api_object_to_id")
def api_object_to_id_fixture():
    """
    Dictionary contains API object to payload object ID mapping
    """
    base_path = "tests/fixtures/request_payloads"
    object_names = [
        "entity",
        "feature_store",
        "event_table",
        "item_table",
        "dimension_table",
        "scd_table",
        "feature_sum_30m",
        "feature_sum_2h",
        "feature_iet",
        "feature_list_single",
        "feature_list_multi",
        "feature_namespace",
        "feature_list_namespace",
        "feature_job_setting_analysis",
        "context",
    ]
    output = {}
    for obj_name in object_names:
        filename = f"{base_path}/{obj_name}.json"
        with open(filename, "r") as fhandle:
            output[obj_name] = json.load(fhandle)["_id"]
    return output


def test_save_payload_fixtures(  # pylint: disable=too-many-arguments
    update_fixtures,
    snowflake_feature_store,
    snowflake_event_table,
    snowflake_item_table,
    snowflake_dimension_table,
    snowflake_scd_table,
    snowflake_event_view_with_entity,
    feature_group,
    non_time_based_feature,
    cust_id_entity,
    transaction_entity,
):
    """
    Write request payload for testing api route
    """
    # pylint: disable=too-many-locals
    feature_sum_30m = feature_group["sum_30m"]
    feature_sum_2h = feature_group["sum_2h"]
    feature_iet = iet_entropy(
        view=snowflake_event_view_with_entity,
        group_by_col="cust_id",
        window="24h",
        name="iet_entropy_24h",
        feature_job_setting=FeatureJobSetting(
            frequency="6h",
            time_modulo_frequency="3h",
            blind_spot="3h",
        ),
    )
    snowflake_item_table.event_id_col.as_entity(transaction_entity.name)
    item_view = snowflake_item_table.get_view(event_suffix="_event_table")
    feature_item_event = item_view.groupby("event_id_col").aggregate(
        value_column="cust_id_event_table",
        method=AggFunc.SUM,
        feature_name="item_event_sum_cust_id_feature",
    )

    feature_list = FeatureList([feature_sum_30m], name="sf_feature_list")
    feature_list_multiple = FeatureList(
        [feature_sum_30m, feature_sum_2h], name="sf_feature_list_multiple"
    )
    feature_namespace = FeatureNamespaceCreate(
        _id=ObjectId(),
        name=feature_sum_30m.name,
        dtype=DBVarType.FLOAT,
        feature_ids=[feature_sum_30m.id],
        readiness=FeatureReadiness.DRAFT,
        default_feature_id=feature_sum_30m.id,
        entity_ids=feature_sum_30m.entity_ids,
        table_ids=feature_sum_30m.table_ids,
    )
    feature_list_namespace = FeatureListNamespaceModel(
        _id=ObjectId(),
        name=feature_list_multiple.name,
        feature_list_ids=[feature_list_multiple.id],
        dtype_distribution=[{"dtype": "FLOAT", "count": 2}],
        readiness_distribution=[{"readiness": "DRAFT", "count": 2}],
        default_feature_list_id=feature_list_multiple.id,
        default_version_mode=DefaultVersionMode.AUTO,
        entity_ids=feature_sum_30m.entity_ids,
        table_ids=feature_sum_30m.table_ids,
        feature_namespace_ids=[
            feature_sum_30m.feature_namespace_id,
            feature_sum_2h.feature_namespace_id,
        ],
    )
    feature_job_setting_analysis = FeatureJobSettingAnalysisCreate(
        _id="62f301e841b73757c9ff879a",
        user_id="62f302f841b73757c9ff876b",
        name="sample_analysis",
        event_table_id=snowflake_event_table.id,
        analysis_data=None,
        analysis_length=2419200,
        min_featurejob_period=60,
        exclude_late_job=False,
        blind_spot_buffer_setting=5,
        job_time_buffer_setting="auto",
        late_data_allowance=5e-05,
    )
    context = ContextCreate(name="transaction_context", entity_ids=[cust_id_entity.id])
    deployment = DeploymentCreate(name=None, feature_list_id=feature_list.id)
    relationship_info = RelationshipInfoCreate(
        _id="63f6a145e549df8ccf123456",
        name="child_parent_relationship",
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=cust_id_entity.id,
        related_entity_id=transaction_entity.id,
        relation_table_id="6337f9651050ee7d5980660d",
        enabled=True,
        updated_by="63f6a145e549df8ccf123444",
    )
    observation_table = ObservationTableCreate(
        name="observation_table",
        feature_store_id=snowflake_feature_store.id,
        request_input=SourceTableRequestInput(
            source=snowflake_event_table.tabular_source,
        ),
        context_id=context.id,
    )
    historical_feature_table = HistoricalFeatureTableCreate(
        name="historical_feature_table",
        feature_store_id=snowflake_feature_store.id,
        feature_list_id=feature_list.id,
        observation_table_id=observation_table.id,
        featurelist_get_historical_features=FeatureListGetHistoricalFeatures(
            feature_clusters=feature_list._get_feature_clusters(),
        ),
    )
    batch_request_table = BatchRequestTableCreate(
        name="batch_request_table",
        feature_store_id=snowflake_feature_store.id,
        request_input=SourceTableRequestInput(
            source=snowflake_dimension_table.tabular_source,
        ),
        context_id=context.id,
    )
    batch_feature_table = BatchFeatureTableCreate(
        name="batch_feature_table",
        feature_store_id=snowflake_feature_store.id,
        batch_request_table_id=batch_request_table.id,
        deployment_id=deployment.id,
    )

    if update_fixtures:
        generated_comment = [
            "THIS IS A GENERATED FILE. DO NOT EDIT THIS FILE DIRECTLY.",
            "Instead, update the test conftest.py#test_save_payload_fixtures.",
            "Run `pytest --update-fixtures` to update it.",
        ]
        api_object_name_pairs = [
            (cust_id_entity, "entity"),
            (transaction_entity, "entity_transaction"),
            (snowflake_feature_store, "feature_store"),
            (snowflake_event_table, "event_table"),
            (snowflake_item_table, "item_table"),
            (snowflake_dimension_table, "dimension_table"),
            (snowflake_scd_table, "scd_table"),
            (feature_sum_30m, "feature_sum_30m"),
            (feature_sum_2h, "feature_sum_2h"),
            (non_time_based_feature, "feature_item_event"),
            (feature_item_event, "feature_item_event"),
            (feature_iet, "feature_iet"),
            (feature_list, "feature_list_single"),
            (feature_list_multiple, "feature_list_multi"),
        ]
        output_filenames = []
        base_path = "tests/fixtures/request_payloads"
        for api_object, name in api_object_name_pairs:
            filename = f"{base_path}/{name}.json"
            with open(filename, "w") as fhandle:
                json_payload = api_object._get_create_payload()
                json_payload["_COMMENT"] = generated_comment
                fhandle.write(json.dumps(json_payload, indent=4, sort_keys=True))
            output_filenames.append(filename)

        schema_payload_name_pairs = [
            (feature_namespace, "feature_namespace"),
            (feature_list_namespace, "feature_list_namespace"),
            (feature_job_setting_analysis, "feature_job_setting_analysis"),
            (context, "context"),
            (deployment, "deployment"),
            (relationship_info, "relationship_info"),
            (observation_table, "observation_table"),
            (historical_feature_table, "historical_feature_table"),
            (batch_request_table, "batch_request_table"),
            (batch_feature_table, "batch_feature_table"),
        ]
        for schema, name in schema_payload_name_pairs:
            filename = f"{base_path}/{name}.json"
            with open(filename, "w") as fhandle:
                json_to_write = schema.json_dict()
                json_to_write["_COMMENT"] = generated_comment
                fhandle.write(json.dumps(json_to_write, indent=4, sort_keys=True))
            output_filenames.append(filename)

        raise AssertionError(
            f"Fixtures {output_filenames} updated, please set update_fixture to False"
        )


@pytest.fixture(name="app_container")
def app_container_fixture(persistent):
    """
    Return an app container used in tests. This will allow us to easily retrieve instances of the right type.
    """
    user = User()
    task_manager = TaskManager(user=user, persistent=persistent, catalog_id=DEFAULT_CATALOG_ID)
    return AppContainer.get_instance(
        user=user,
        persistent=persistent,
        temp_storage=LocalTempStorage(),
        task_manager=task_manager,
        storage=LocalTempStorage(),
        container_id=DEFAULT_CATALOG_ID,
    )


@pytest.fixture(name="get_credential")
def get_credential_fixture(credentials):
    """
    get_credential fixture
    """

    async def get_credential(user_id, feature_store_name):
        _ = user_id
        return credentials.get(feature_store_name)

    return get_credential


@pytest.fixture(autouse=True, scope="function")
def mock_task_manager(request, persistent, storage, temp_storage, get_credential):
    """
    Mock celery task manager for testing
    """
    if "disable_task_manager_mock" in request.keywords:
        yield
    else:
        task_status = {}
        with patch("featurebyte.service.task_manager.TaskManager.submit") as mock_submit:

            async def submit(payload: BaseTaskPayload):
                kwargs = payload.json_dict()
                kwargs["task_output_path"] = payload.task_output_path
                task = TASK_MAP[payload.command](
                    payload=kwargs,
                    progress=Mock(),
                    user=User(id=kwargs.get("user_id")),
                    get_credential=get_credential,
                    get_persistent=lambda: persistent,
                    get_storage=lambda: storage,
                    get_temp_storage=lambda: temp_storage,
                )
                try:
                    await task.execute()
                    status = TaskStatus.SUCCESS
                    traceback_info = None
                except Exception:  # pylint: disable=broad-except
                    status = TaskStatus.FAILURE
                    traceback_info = traceback.format_exc()

                task_id = str(uuid4())
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

            with patch("featurebyte.service.task_manager.celery") as mock_celery:

                def get_task(task_id):
                    status = task_status.get(task_id)
                    if status is None:
                        return None
                    return Mock(status=status)

                mock_celery.AsyncResult.side_effect = get_task
                yield
