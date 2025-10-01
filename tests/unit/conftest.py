"""
Common test fixtures used across unit test directories
"""

import copy
import json
import logging
import os
import tempfile
import traceback
import typing
from datetime import datetime
from functools import partial
from pathlib import Path
from unittest import mock
from unittest.mock import AsyncMock, Mock, PropertyMock, patch
from uuid import UUID, uuid4

import httpx
import pandas as pd
import pytest
import pytest_asyncio
import redis
from bson import ObjectId
from cachetools import TTLCache
from fastapi.testclient import TestClient as BaseTestClient
from snowflake.connector import ProgrammingError
from snowflake.connector.constants import QueryStatus

from featurebyte import (
    CalendarWindow,
    CronFeatureJobSetting,
    FeatureJobSetting,
    MissingValueImputation,
    SnowflakeDetails,
    UsernamePasswordCredential,
)
from featurebyte.api.api_object import ApiObject
from featurebyte.api.catalog import Catalog
from featurebyte.api.context import Context
from featurebyte.api.entity import Entity
from featurebyte.api.feature import Feature
from featurebyte.api.feature_group import FeatureGroup
from featurebyte.api.feature_list import FeatureList
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.groupby import GroupBy
from featurebyte.api.item_table import ItemTable
from featurebyte.api.online_store import OnlineStore
from featurebyte.api.request_column import RequestColumn
from featurebyte.app import User, app, get_celery
from featurebyte.enum import AggFunc, InternalName, SourceType
from featurebyte.exception import DuplicatedRecordException, ObjectHasBeenSavedError
from featurebyte.logging import CONSOLE_LOG_FORMATTER
from featurebyte.models.credential import CredentialModel
from featurebyte.models.development_dataset import DevelopmentDatasetModel, DevelopmentTable
from featurebyte.models.feature_namespace import FeatureReadiness
from featurebyte.models.online_store import MySQLOnlineStoreDetails
from featurebyte.models.periodic_task import Crontab
from featurebyte.models.system_metrics import TileComputeMetrics
from featurebyte.models.task import Task as TaskModel
from featurebyte.models.tile import OnDemandTileComputeResult, TileSpec
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.dtype import PartitionMetadata
from featurebyte.query_graph.model.time_series_table import TimeInterval
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema, TimeZoneColumn
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.feature_historical import HistoricalFeatureQueryGenerator
from featurebyte.query_graph.sql.online_store_compute_query import (
    get_online_store_precompute_queries,
)
from featurebyte.query_graph.sql.source_info import SourceInfo
from featurebyte.routes.lazy_app_container import LazyAppContainer
from featurebyte.routes.registry import app_container_config
from featurebyte.schema.catalog import CatalogCreate
from featurebyte.schema.task import TaskStatus
from featurebyte.schema.worker.task.base import BaseTaskPayload
from featurebyte.session.base import DEFAULT_EXECUTE_QUERY_TIMEOUT_SECONDS
from featurebyte.session.snowflake import SnowflakeSession
from featurebyte.storage.local import LocalStorage
from featurebyte.worker.registry import TASK_REGISTRY_MAP
from featurebyte.worker.test_util.random_task import Command, LongRunningTask
from tests.unit.conftest_config import (
    config_file_fixture,
    config_fixture,
    mock_config_path_env_fixture,
)
from tests.util.helper import inject_request_side_effect, safe_freeze_time

# register tests.unit.routes.base so that API stacktrace display properly
pytest.register_assert_rewrite("tests.unit.routes.base")

# "Registering" fixtures so that they'll be available for use as if they were defined here.
# We keep the definition in a separate file for readability
_ = [config_file_fixture, config_fixture, mock_config_path_env_fixture]

TEST_REDIS_URI = "redis://localhost:36379"


class Response(httpx.Response):
    """
    Response object with additional methods
    """

    def __init__(self, response: httpx.Response):
        for key, value in response.__dict__.items():
            setattr(self, key, value)

    def iter_content(self, chunk_size=1):
        return super().iter_bytes(chunk_size=chunk_size)


class TestClient(BaseTestClient):
    """
    Override TestClient to handle streaming responses
    """

    def request(
        self,
        method: str,
        url: httpx._types.URLTypes,
        *args: typing.Any,
        stream: bool = False,
        **kwargs: typing.Any,
    ) -> httpx.Response:
        """
        Override request method to handle streaming responses
        """
        if "allow_redirects" in kwargs:
            kwargs["follow_redirects"] = kwargs.pop("allow_redirects")
        if stream:
            with super().stream(method, url, *args, **kwargs) as response:
                return Response(response)
        return super().request(method, url, *args, **kwargs)


@pytest.fixture(name="mock_api_object_cache")
def mock_api_object_cache_fixture():
    """Mock api object cache so that the time-to-live period is 0"""
    with patch.object(ApiObject, "_cache", new_callable=PropertyMock) as mock_cache:
        mock_cache.return_value = TTLCache(maxsize=1024, ttl=0)
        yield


@pytest.fixture(autouse=True, scope="session")
def mock_api_client_fixture(request, user_id):
    """
    Mock Configurations.get_client to use test client
    """
    if "no_mock_api_client" in request.keywords:
        yield
    else:
        with mock.patch("featurebyte.app.User") as mock_user:
            mock_user.return_value = User(id=user_id)
            with mock.patch("featurebyte.config.BaseAPIClient.request") as mock_request:
                with TestClient(app) as client:
                    yield inject_request_side_effect(mock_request, client)


@pytest.fixture(autouse=True)
def mock_websocket_client_fixture(request):
    """
    Mock Configurations.get_websocket_client to use test client
    """
    if "no_mock_websocket_client" in request.keywords:
        yield
    else:
        with mock.patch(
            "featurebyte.config.Configurations.get_websocket_client"
        ) as mock_get_websocket_client:
            mock_get_websocket_client.return_value.__enter__.return_value.receive_json.return_value = None
            yield mock_get_websocket_client


@pytest.fixture(name="mock_redis", autouse=True)
def mock_redis_fixture():
    """Mock get_redis in featurebyte.worker"""
    with patch("featurebyte.worker.Redis") as mock_get_redis:
        mock_redis = mock_get_redis.from_url.return_value
        mock_redis.pipeline.return_value.execute.return_value = [0]
        mock_redis.zrank.return_value = 0
        yield mock_redis


def get_increasing_object_id_callable():
    """
    Get a callable that returns an increasing object id
    """

    current = 0

    def increasing_object_id():
        # 0-pad with 24 characters
        nonlocal current
        out = f"{current:024d}"
        current += 1
        return out

    return increasing_object_id


@pytest.fixture(autouse=True)
def patched_query_cache_unique_identifier():
    """
    Patch ObjectId to return a fixed value
    """
    with patch(
        "featurebyte.service.query_cache_manager.ObjectId",
        side_effect=get_increasing_object_id_callable(),
    ):
        yield


@pytest.fixture(autouse=True)
def patched_sql_common_unique_identifier():
    """
    Fixture to mock ObjectId to a fixed value
    """
    with patch("featurebyte.sql.common.ObjectId", side_effect=get_increasing_object_id_callable()):
        yield


@pytest.fixture(autouse=True)
def patched_tile_generate_unique_identifier():
    """
    Fixture to mock ObjectId to a fixed value
    """
    with patch(
        "featurebyte.sql.tile_generate.ObjectId", side_effect=get_increasing_object_id_callable()
    ):
        yield


@pytest.fixture(autouse=True)
def patched_tile_cache_query_by_observation_table_unique_identifier():
    """
    Fixture to mock ObjectId to a fixed value
    """
    with patch(
        "featurebyte.service.tile_cache_query_by_observation_table.ObjectId",
        side_effect=get_increasing_object_id_callable(),
    ):
        yield


@pytest.fixture(autouse=True)
def patched_feature_table_cache_metadata_unique_identifier():
    """
    Fixture to mock ObjectId to a fixed value
    """
    with patch(
        "featurebyte.service.feature_table_cache_metadata.ObjectId",
        side_effect=get_increasing_object_id_callable(),
    ):
        yield


@pytest.fixture(autouse=True)
def patched_cron_helper_unique_identifier():
    """
    Fixture to mock ObjectId to a fixed value
    """
    with patch(
        "featurebyte.service.cron_helper.ObjectId",
        side_effect=get_increasing_object_id_callable(),
    ):
        yield


@pytest.fixture(autouse=True)
def patched_execute_feature_query_set_unique_identifier():
    """
    Fixture to mock ObjectId to a fixed value
    """
    with patch(
        "featurebyte.session.session_helper.ObjectId",
        side_effect=get_increasing_object_id_callable(),
    ):
        yield


@pytest.fixture(autouse=True)
def patched_preview_service_unique_identifier():
    """
    Fixture to mock ObjectId to a fixed value
    """
    with patch(
        "featurebyte.service.preview.ObjectId",
        side_effect=get_increasing_object_id_callable(),
    ):
        yield


@pytest.fixture(autouse=True)
def patched_get_feature_query_unique_identifier():
    """
    Fixture to mock ObjectId to a fixed value
    """
    with patch(
        "featurebyte.query_graph.sql.feature_compute.ObjectId",
        side_effect=get_increasing_object_id_callable(),
    ):
        yield


@pytest.fixture(autouse=True)
def patched_deployed_tile_table_manager_unique_identifier():
    """
    Fixture to mock ObjectId to a fixed value
    """
    with patch(
        "featurebyte.service.deployed_tile_table_manager.ObjectId",
        side_effect=get_increasing_object_id_callable(),
    ):
        yield


@pytest.fixture(autouse=True)
def patched_materialized_table_location_unique_identifier():
    """
    Fixture to mock ObjectId to a fixed value
    """
    with patch(
        "featurebyte.service.materialized_table.ObjectId",
        side_effect=get_increasing_object_id_callable(),
    ):
        yield


@pytest.fixture(name="storage")
def storage_fixture():
    """
    Storage object fixture
    """
    with tempfile.TemporaryDirectory(suffix=f"_{ObjectId()}") as tempdir:
        yield LocalStorage(base_path=Path(tempdir))


@pytest.fixture(name="temp_storage")
def temp_storage_fixture():
    """
    Storage object fixture
    """
    with tempfile.TemporaryDirectory(suffix=f"_{ObjectId()}") as tempdir:
        yield LocalStorage(base_path=Path(tempdir))


@pytest.fixture(name="mock_get_persistent")
def mock_get_persistent_function(mongo_persistent):
    """
    Mock get_persistent in featurebyte.app
    """
    with mock.patch("featurebyte.app.MongoDBImpl") as mock_persistent:
        persistent, _ = mongo_persistent
        mock_persistent.return_value = persistent
        yield mock_persistent


@pytest.fixture(autouse=True)
def mock_settings_env_vars(mock_config_path_env, mock_get_persistent):
    """Use these fixtures for all tests"""
    _ = mock_config_path_env, mock_get_persistent
    yield


@pytest.fixture(name="snowflake_connector_patches")
def snowflake_connector_patches():
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
        yield {
            "connector": mock_connector,
            "cursor": cursor,
        }


@pytest.fixture(name="snowflake_connector")
def snowflake_connector_fixture(snowflake_connector_patches):
    """
    Mock snowflake connector cursor
    """
    yield snowflake_connector_patches["connector"]


@pytest.fixture(name="snowflake_connector_cursor")
def snowflake_connector_cursor_fixture(snowflake_connector_patches):
    """
    Mock snowflake connector cursor
    """
    yield snowflake_connector_patches["cursor"]


@pytest.fixture(name="snowflake_query_map")
def snowflake_query_map_fixture():
    """snowflake query map fixture"""
    query_map = {
        "SELECT DATABASE_NAME FROM INFORMATION_SCHEMA.DATABASES": [
            {"DATABASE_NAME": "sf_database"}
        ],
        'SELECT SCHEMA_NAME FROM "sf_database".INFORMATION_SCHEMA.SCHEMATA': [
            {"SCHEMA_NAME": "sf_schema"}
        ],
        (
            'SELECT TABLE_NAME, COMMENT FROM "sf_database".INFORMATION_SCHEMA.TABLES WHERE '
            "TABLE_SCHEMA = 'sf_schema'"
        ): [
            {"TABLE_NAME": "sf_table", "COMMENT": ""},
            {"TABLE_NAME": "sf_table_no_tz", "COMMENT": None},
            {"TABLE_NAME": "items_table", "COMMENT": "Item table"},
            {"TABLE_NAME": "items_table_same_event_id", "COMMENT": None},
            {"TABLE_NAME": "fixed_table", "COMMENT": None},
            {"TABLE_NAME": "non_scalar_table", "COMMENT": None},
            {"TABLE_NAME": "scd_table", "COMMENT": "SCD table"},
            {"TABLE_NAME": "scd_table_state_map", "COMMENT": None},
            {"TABLE_NAME": "dimension_table", "COMMENT": "Dimension table"},
            {"TABLE_NAME": "sf_view", "COMMENT": "this is view"},
        ],
        'SHOW COLUMNS IN "sf_database"."sf_schema"."sf_table"': [
            {
                "column_name": "col_int",
                "data_type": json.dumps({"type": "FIXED", "scale": 0}),
                "comment": None,
            },
            {
                "column_name": "col_float",
                "data_type": json.dumps({"type": "REAL"}),
                "comment": "Float column",
            },
            {
                "column_name": "col_char",
                "data_type": json.dumps({"type": "TEXT", "length": 1}),
                "comment": "Char column",
            },
            {
                "column_name": "col_text",
                "data_type": json.dumps({"type": "TEXT", "length": 2**24}),
                "comment": "Text column",
            },
            {
                "column_name": "col_binary",
                "data_type": json.dumps({"type": "BINARY"}),
                "comment": None,
            },
            {
                "column_name": "col_boolean",
                "data_type": json.dumps({"type": "BOOLEAN"}),
                "comment": None,
            },
            {
                "column_name": "event_timestamp",
                "data_type": json.dumps({"type": "TIMESTAMP_TZ"}),
                "comment": "Timestamp column",
            },
            {
                "column_name": "created_at",
                "data_type": json.dumps({"type": "TIMESTAMP_TZ"}),
                "comment": None,
            },
            {
                "column_name": "cust_id",
                "data_type": json.dumps({"type": "FIXED", "scale": 0}),
                "comment": None,
            },
        ],
        'SHOW COLUMNS IN "sf_database"."sf_schema"."sf_table_no_tz"': [
            {
                "column_name": "event_timestamp",
                "data_type": json.dumps({"type": "TIMESTAMP_NTZ"}),
                "comment": None,
            },
            {
                "column_name": "created_at",
                "data_type": json.dumps({"type": "TIMESTAMP_NTZ"}),
                "comment": None,
            },
            {
                "column_name": "col_int",
                "data_type": json.dumps({"type": "FIXED", "scale": 0}),
                "comment": None,
            },
            {
                "column_name": "cust_id",
                "data_type": json.dumps({"type": "FIXED", "scale": 0}),
                "comment": None,
            },
            {
                "column_name": "tz_offset",
                "data_type": json.dumps({"type": "TEXT", "length": 2**24}),
                "comment": None,
            },
        ],
        'SHOW COLUMNS IN "sf_database"."sf_schema"."sf_view"': [
            {"column_name": "col_date", "data_type": json.dumps({"type": "DATE"}), "comment": None},
            {"column_name": "col_time", "data_type": json.dumps({"type": "TIME"}), "comment": None},
            {
                "column_name": "col_timestamp_ltz",
                "data_type": json.dumps({"type": "TIMESTAMP_LTZ"}),
                "comment": "Timestamp ltz column",
            },
            {
                "column_name": "col_timestamp_ntz",
                "data_type": json.dumps({"type": "TIMESTAMP_NTZ"}),
                "comment": None,
            },
            {
                "column_name": "col_timestamp_tz",
                "data_type": json.dumps({"type": "TIMESTAMP_TZ"}),
                "comment": "Timestamp tz column",
            },
        ],
        'SHOW COLUMNS IN "sf_database"."sf_schema"."items_table"': [
            {
                "column_name": "event_id_col",
                "data_type": json.dumps({"type": "FIXED", "scale": 0}),
                "comment": None,
            },
            {
                "column_name": "item_id_col",
                "data_type": json.dumps({"type": "TEXT", "length": 2**24}),
                "comment": None,
            },
            {
                "column_name": "item_type",
                "data_type": json.dumps({"type": "TEXT", "length": 2**24}),
                "comment": None,
            },
            {
                "column_name": "item_amount",
                "data_type": json.dumps({"type": "REAL"}),
                "comment": None,
            },
            {
                "column_name": "created_at",
                "data_type": json.dumps({"type": "TIMESTAMP_TZ"}),
                "comment": None,
            },
            {
                "column_name": "event_timestamp",
                "data_type": json.dumps({"type": "TIMESTAMP_TZ"}),
                "comment": None,
            },
        ],
        'SHOW COLUMNS IN "sf_database"."sf_schema"."items_table_same_event_id"': [
            {
                "column_name": "col_int",
                "data_type": json.dumps({"type": "FIXED", "scale": 0}),
                "comment": None,
            },
            {
                "column_name": "item_id_col",
                "data_type": json.dumps({"type": "TEXT", "length": 2**24}),
                "comment": None,
            },
            {
                "column_name": "created_at",
                "data_type": json.dumps({"type": "TIMESTAMP_TZ"}),
                "comment": None,
            },
        ],
        'SHOW COLUMNS IN "sf_database"."sf_schema"."fixed_table"': [
            {
                "column_name": "num",
                "data_type": json.dumps({"type": "FIXED", "scale": 0}),
                "comment": None,
            },
            {
                "column_name": "num10",
                "data_type": json.dumps({"type": "FIXED", "scale": 1}),
                "comment": None,
            },
            {
                "column_name": "dec",
                "data_type": json.dumps({"type": "FIXED", "scale": 2}),
                "comment": None,
            },
        ],
        'SHOW COLUMNS IN "sf_database"."sf_schema"."non_scalar_table"': [
            {
                "column_name": "variant",
                "data_type": json.dumps({"type": "VARIANT", "nullable": True}),
                "comment": None,
            },
        ],
        'SHOW COLUMNS IN "sf_database"."sf_schema"."scd_table"': [
            {
                "column_name": "col_int",
                "data_type": json.dumps({"type": "FIXED", "scale": 0}),
                "comment": None,
            },
            {
                "column_name": "col_float",
                "data_type": json.dumps({"type": "REAL"}),
                "comment": None,
            },
            {
                "column_name": "is_active",
                "data_type": json.dumps({"type": "BOOLEAN", "length": 1}),
                "comment": None,
            },
            {
                "column_name": "col_text",
                "data_type": json.dumps({"type": "TEXT", "length": 2**24}),
                "comment": None,
            },
            {
                "column_name": "col_binary",
                "data_type": json.dumps({"type": "BINARY"}),
                "comment": None,
            },
            {
                "column_name": "col_boolean",
                "data_type": json.dumps({"type": "BOOLEAN"}),
                "comment": None,
            },
            {
                "column_name": "effective_timestamp",
                "data_type": json.dumps({"type": "TIMESTAMP_TZ"}),
                "comment": None,
            },
            {
                "column_name": "end_timestamp",
                "data_type": json.dumps({"type": "TIMESTAMP_TZ"}),
                "comment": None,
            },
            {
                "column_name": "date_of_birth",
                "data_type": json.dumps({"type": "TIMESTAMP_NTZ"}),
                "comment": None,
            },
            {
                "column_name": "created_at",
                "data_type": json.dumps({"type": "TIMESTAMP_TZ"}),
                "comment": None,
            },
            {
                "column_name": "cust_id",
                "data_type": json.dumps({"type": "FIXED", "scale": 0}),
                "comment": None,
            },
        ],
        'SHOW COLUMNS IN "sf_database"."sf_schema"."time_series_table"': [
            {
                "column_name": "col_int",
                "data_type": json.dumps({"type": "FIXED", "scale": 0}),
                "comment": None,
            },
            {
                "column_name": "col_float",
                "data_type": json.dumps({"type": "REAL"}),
                "comment": "Float column",
            },
            {
                "column_name": "col_char",
                "data_type": json.dumps({"type": "TEXT", "length": 1}),
                "comment": "Char column",
            },
            {
                "column_name": "col_text",
                "data_type": json.dumps({"type": "TEXT", "length": 2**24}),
                "comment": "Text column",
            },
            {
                "column_name": "col_binary",
                "data_type": json.dumps({"type": "BINARY"}),
                "comment": None,
            },
            {
                "column_name": "col_boolean",
                "data_type": json.dumps({"type": "BOOLEAN"}),
                "comment": None,
            },
            {
                "column_name": "date",
                "data_type": json.dumps({"type": "TEXT", "length": 8}),
                "comment": "Date column",
            },
            {
                "column_name": "created_at",
                "data_type": json.dumps({"type": "TIMESTAMP_TZ"}),
                "comment": None,
            },
            {
                "column_name": "store_id",
                "data_type": json.dumps({"type": "FIXED", "scale": 0}),
                "comment": None,
            },
            {
                "column_name": "another_timestamp_col",
                "data_type": json.dumps({"type": "TIMESTAMP_TZ"}),
                "comment": None,
            },
        ],
        'SHOW COLUMNS IN "sf_database"."sf_schema"."scd_table_state_map"': [
            {
                "column_name": "col_int",
                "data_type": json.dumps({"type": "FIXED", "scale": 0}),
                "comment": None,
            },
            {
                "column_name": "is_active",
                "data_type": json.dumps({"type": "BOOLEAN", "length": 1}),
                "comment": None,
            },
            {
                "column_name": "col_text",
                "data_type": json.dumps({"type": "TEXT", "length": 2**24}),
                "comment": None,
            },
            {
                "column_name": "effective_timestamp",
                "data_type": json.dumps({"type": "TIMESTAMP_TZ"}),
                "comment": None,
            },
            {
                "column_name": "end_timestamp",
                "data_type": json.dumps({"type": "TIMESTAMP_TZ"}),
                "comment": None,
            },
            {
                "column_name": "col_boolean",
                "data_type": json.dumps({"type": "BOOLEAN"}),
                "comment": None,
            },
            {
                "column_name": "state_code",
                "data_type": json.dumps({"type": "FIXED", "scale": 0}),
                "comment": None,
            },
        ],
        "SHOW SCHEMAS": [
            {"name": "PUBLIC"},
        ],
        (
            'SELECT * FROM "database"."INFORMATION_SCHEMA"."TABLES" WHERE '
            "\"TABLE_SCHEMA\"='schema' AND \"TABLE_NAME\"='table'"
        ): [
            {
                "TABLE_NAME": "table",
                "TABLE_SCHEMA": "schema",
                "TABLE_CATALOG": "database",
                "TABLE_TYPE": "VIEW",
                "COMMENT": None,
            }
        ],
        (
            'SELECT * FROM "sf_database"."INFORMATION_SCHEMA"."TABLES" WHERE '
            "\"TABLE_SCHEMA\"='sf_schema' AND \"TABLE_NAME\"='sf_table'"
        ): [
            {
                "TABLE_NAME": "sf_table",
                "TABLE_SCHEMA": "sf_schema",
                "TABLE_CATALOG": "sf_database",
                "TABLE_TYPE": "VIEW",
                "COMMENT": None,
            }
        ],
        (
            'SELECT * FROM "sf_database"."INFORMATION_SCHEMA"."TABLES" WHERE '
            "\"TABLE_SCHEMA\"='sf_schema' AND \"TABLE_NAME\"='sf_table_no_tz'"
        ): [
            {
                "TABLE_NAME": "sf_table_no_tz",
                "TABLE_SCHEMA": "sf_schema",
                "TABLE_CATALOG": "sf_database",
                "TABLE_TYPE": "VIEW",
                "COMMENT": None,
            }
        ],
        (
            'SELECT * FROM "sf_database"."INFORMATION_SCHEMA"."TABLES" WHERE '
            "\"TABLE_SCHEMA\"='sf_schema' AND \"TABLE_NAME\"='items_table_same_event_id'"
        ): [
            {
                "TABLE_NAME": "sf_table_no_tz",
                "TABLE_SCHEMA": "sf_schema",
                "TABLE_CATALOG": "sf_database",
                "TABLE_TYPE": "VIEW",
                "COMMENT": None,
            }
        ],
        (
            'SELECT * FROM "sf_database"."INFORMATION_SCHEMA"."TABLES" WHERE '
            "\"TABLE_SCHEMA\"='sf_schema' AND \"TABLE_NAME\"='scd_table'"
        ): [
            {
                "TABLE_NAME": "sf_scd_table",
                "TABLE_SCHEMA": "sf_schema",
                "TABLE_CATALOG": "sf_database",
                "TABLE_TYPE": "VIEW",
                "COMMENT": "SCD table",
            }
        ],
        (
            'SELECT * FROM "sf_database"."INFORMATION_SCHEMA"."TABLES" WHERE '
            "\"TABLE_SCHEMA\"='sf_schema' AND \"TABLE_NAME\"='scd_table_state_map'"
        ): [
            {
                "TABLE_NAME": "sf_scd_table",
                "TABLE_SCHEMA": "sf_schema",
                "TABLE_CATALOG": "sf_database",
                "TABLE_TYPE": "VIEW",
                "COMMENT": None,
            }
        ],
        (
            'SELECT * FROM "sf_database"."INFORMATION_SCHEMA"."TABLES" WHERE '
            "\"TABLE_SCHEMA\"='sf_schema' AND \"TABLE_NAME\"='scd_table_v2'"
        ): [
            {
                "TABLE_NAME": "sf_scd_table",
                "TABLE_SCHEMA": "sf_schema",
                "TABLE_CATALOG": "sf_database",
                "TABLE_TYPE": "VIEW",
                "COMMENT": None,
            }
        ],
        (
            'SELECT * FROM "sf_database"."INFORMATION_SCHEMA"."TABLES" WHERE '
            "\"TABLE_SCHEMA\"='sf_schema' AND \"TABLE_NAME\"='items_table'"
        ): [
            {
                "TABLE_NAME": "sf_table",
                "TABLE_SCHEMA": "sf_schema",
                "TABLE_CATALOG": "sf_database",
                "TABLE_TYPE": "VIEW",
                "COMMENT": "Item table",
            }
        ],
        (
            'SELECT * FROM "sf_database"."INFORMATION_SCHEMA"."TABLES" WHERE '
            "\"TABLE_SCHEMA\"='sf_schema' AND \"TABLE_NAME\"='dimension_table'"
        ): [
            {
                "TABLE_NAME": "sf_table",
                "TABLE_SCHEMA": "sf_schema",
                "TABLE_CATALOG": "sf_database",
                "TABLE_TYPE": "VIEW",
                "COMMENT": "Dimension table",
            }
        ],
        (
            'SELECT * FROM "sf_database"."INFORMATION_SCHEMA"."TABLES" WHERE '
            "\"TABLE_SCHEMA\"='sf_schema' AND \"TABLE_NAME\"='time_series_table'"
        ): [
            {
                "TABLE_NAME": "time_series_table",
                "TABLE_SCHEMA": "sf_schema",
                "TABLE_CATALOG": "sf_database",
                "TABLE_TYPE": "VIEW",
                "COMMENT": "SCD table",
            }
        ],
        (
            'SELECT * FROM "sf_database"."INFORMATION_SCHEMA"."TABLES" WHERE '
            "\"TABLE_SCHEMA\"='sf_schema' AND \"TABLE_NAME\"='snapshots_table'"
        ): [
            {
                "TABLE_NAME": "snapshits_table",
                "TABLE_SCHEMA": "sf_schema",
                "TABLE_CATALOG": "sf_database",
                "TABLE_TYPE": "VIEW",
                "COMMENT": "Snapshots table",
            }
        ],
        "SELECT WORKING_SCHEMA_VERSION, FEATURE_STORE_ID FROM METADATA_SCHEMA": [],
        'SELECT\n  COUNT(*) AS "row_count"\nFROM "sf_database"."sf_schema"."sf_table"': [
            {"row_count": 100}
        ],
        (
            'SELECT\n  COUNT(*) AS "row_count"\nFROM (\n  SELECT\n    "event_timestamp" AS "POINT_IN_TIME",'
            '\n    "cust_id" AS "cust_id"\n  FROM (\n    SELECT\n      "col_int" AS "col_int",\n      '
            '"col_float" AS "col_float",\n      "col_char" AS "col_char",\n      "col_text" AS "col_text",'
            '\n      "col_binary" AS "col_binary",\n      "col_boolean" AS "col_boolean",\n      '
            '"event_timestamp" AS "event_timestamp",\n      "cust_id" AS "cust_id"\n    '
            'FROM "sf_database"."sf_schema"."sf_table"\n  )\n)'
        ): [{"row_count": 100}],
        'SELECT\n  *\nFROM "sf_database"."sf_schema"."sf_table"\nLIMIT 3': [
            {
                "col_int": [1, 2, 3],
                "col_float": [1.0, 2.0, 3.0],
                "col_char": ["a", "b", "c"],
                "col_text": ["abc", "def", "ghi"],
                "col_binary": [1, 0, 1],
                "col_boolean": [True, False, True],
                "event_timestamp": [
                    "2021-01-01 00:00:00",
                    "2021-01-01 00:00:00",
                    "2021-01-01 00:00:00",
                ],
                "created_at": [
                    "2021-01-01 00:00:00",
                    "2021-01-01 00:00:00",
                    "2021-01-01 00:00:00",
                ],
                "cust_id": [1, 2, 3],
            }
        ],
        # For time series table validation
        'SELECT\n  COUNT(DISTINCT "date") AS "date"\nFROM "sf_database"."sf_schema"."time_series_table"': [
            {"date": 10}
        ],
        "SHOW WAREHOUSES": [
            {
                "name": "sf_warehouse",
                "state": "ACTIVE",
                "type": "STANDARD",
                "size": "X-SMALL",
                "auto_suspend": 60,
                "auto_resume": True,
            },
            {
                "name": "alt_warehouse",
                "state": "ACTIVE",
                "type": "STANDARD",
                "size": "X-SMALL",
                "auto_suspend": 60,
                "auto_resume": True,
            },
        ],
        'SHOW COLUMNS IN "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000000"': [
            {
                "column_name": "col_int",
                "data_type": json.dumps({"type": "FIXED", "scale": 0}),
                "comment": None,
            },
            {
                "column_name": "col_float",
                "data_type": json.dumps({"type": "REAL"}),
                "comment": "Float column",
            },
            {
                "column_name": "col_char",
                "data_type": json.dumps({"type": "TEXT", "length": 1}),
                "comment": "Char column",
            },
            {
                "column_name": "col_text",
                "data_type": json.dumps({"type": "TEXT", "length": 2**24}),
                "comment": "Text column",
            },
            {
                "column_name": "col_binary",
                "data_type": json.dumps({"type": "BINARY"}),
                "comment": None,
            },
            {
                "column_name": "col_boolean",
                "data_type": json.dumps({"type": "BOOLEAN"}),
                "comment": None,
            },
            {
                "column_name": "event_timestamp",
                "data_type": json.dumps({"type": "TIMESTAMP_TZ"}),
                "comment": "Timestamp column",
            },
            {
                "column_name": "created_at",
                "data_type": json.dumps({"type": "TIMESTAMP_TZ"}),
                "comment": None,
            },
            {
                "column_name": "cust_id",
                "data_type": json.dumps({"type": "FIXED", "scale": 0}),
                "comment": None,
            },
            {
                "column_name": "__FB_TABLE_ROW_INDEX",
                "data_type": json.dumps({"type": "FIXED", "scale": 0}),
                "comment": None,
            },
        ],
    }
    query_map['SHOW COLUMNS IN "sf_database"."sf_schema"."dimension_table"'] = query_map[
        'SHOW COLUMNS IN "sf_database"."sf_schema"."sf_table"'
    ]
    query_map['SHOW COLUMNS IN "sf_database"."sf_schema"."scd_table_v2"'] = (
        get_show_columns_query_result_for_scd_table_v2(
            query_map['SHOW COLUMNS IN "sf_database"."sf_schema"."scd_table"']
        )
    )
    query_map['SHOW COLUMNS IN "sf_database"."sf_schema"."snapshots_table"'] = query_map[
        'SHOW COLUMNS IN "sf_database"."sf_schema"."time_series_table"'
    ]
    query_map['SHOW COLUMNS IN "sf_database"."sf_schema"."another_snapshots_table"'] = query_map[
        'SHOW COLUMNS IN "sf_database"."sf_schema"."time_series_table"'
    ]
    return query_map


def get_show_columns_query_result_for_scd_table_v2(scd_table_query_result):
    """
    Get SHOW COLUMNS query result for scd_table_v2 where effective_timestamp is renamed to
    event_timestamp
    """
    result = copy.deepcopy(scd_table_query_result)
    for info in result:
        if info["column_name"] == "effective_timestamp":
            info["column_name"] = "event_timestamp"
    return result


@pytest.fixture(name="snowflake_execute_query")
def mock_snowflake_execute_query(snowflake_connector, snowflake_query_map):
    """
    Mock execute_query in featurebyte.session.snowflake.SnowflakeSession class
    """
    _ = snowflake_connector

    def side_effect(
        query, timeout=DEFAULT_EXECUTE_QUERY_TIMEOUT_SECONDS, to_log_error=True, query_metadata=None
    ):
        _ = timeout, to_log_error, query_metadata
        res = snowflake_query_map.get(query)
        if res is not None:
            return pd.DataFrame(res)
        print(query)
        return None

    with mock.patch(
        "featurebyte.session.snowflake.SnowflakeSession.execute_query"
    ) as mock_execute_query:
        mock_execute_query.side_effect = side_effect
        yield mock_execute_query


@pytest.fixture(name="snowflake_feature_store_params")
def snowflake_feature_store_params():
    """
    Snowflake database source params fixture
    """
    return {
        "name": "sf_featurestore",
        "source_type": "snowflake",
        "details": SnowflakeDetails(
            account="sf_account",
            warehouse="sf_warehouse",
            schema_name="sf_schema",
            database_name="sf_database",
            role_name="TESTING",
        ),
        "database_credential": UsernamePasswordCredential(
            username="sf_user",
            password="sf_password",
        ),
    }


@pytest.fixture(name="patched_to_thread")
def patched_to_thread_fixture():
    """
    Patch to_thread function to run the function synchronously in session manager to avoid tests
    hanging due to asyncio
    """

    def _patched_to_thread(func, timeout, error_handler, *args, **kwargs):
        _ = timeout
        _ = error_handler
        return func(*args, **kwargs)

    with patch("featurebyte.service.session_manager.to_thread", side_effect=_patched_to_thread):
        yield


@pytest.fixture(name="snowflake_feature_store")
def snowflake_feature_store(
    snowflake_feature_store_params,
    snowflake_execute_query,
    snowflake_feature_store_id,
    patched_to_thread,
):
    """
    Snowflake database source fixture
    """
    _ = snowflake_execute_query
    params = snowflake_feature_store_params.copy()
    try:
        params["_id"] = snowflake_feature_store_id
        params["type"] = "snowflake"
        feature_store = FeatureStore(**params)
        feature_store.save()
        return feature_store
    except (DuplicatedRecordException, ObjectHasBeenSavedError):
        return FeatureStore.get(params["name"])


@pytest.fixture(name="snowflake_credentials")
def snowflake_credentials(snowflake_feature_store_params):
    """
    Credentials fixture
    """
    return CredentialModel(
        name="sf_featurestore",
        group_ids=[],
        feature_store_id=ObjectId(),
        database_credential=UsernamePasswordCredential(
            username="sf_user",
            password="sf_password",
        ),
    )


@pytest.fixture(name="snowflake_data_source")
def snowflake_data_source_fixture(snowflake_feature_store):
    """
    Snowflake table source fixture
    """
    return snowflake_feature_store.get_data_source()


@pytest.fixture(name="snowflake_database_table")
def snowflake_database_table_fixture(snowflake_data_source):
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


@pytest.fixture(name="snowflake_database_time_series_table")
def snowflake_database_time_series_table_fixture(snowflake_data_source):
    """
    SourceTable object fixture for TimeSeriesTable
    """
    yield snowflake_data_source.get_source_table(
        database_name="sf_database",
        schema_name="sf_schema",
        table_name="time_series_table",
    )


@pytest.fixture(name="snowflake_database_snapshots_table")
def snowflake_database_snapshots_table_fixture(snowflake_data_source):
    """
    SourceTable object fixture for SnapshotsTable
    """
    yield snowflake_data_source.get_source_table(
        database_name="sf_database",
        schema_name="sf_schema",
        table_name="snapshots_table",
    )


@pytest.fixture(name="another_snowflake_database_snapshots_table")
def another_snowflake_database_snapshots_table_fixture(snowflake_data_source):
    """
    SourceTable object fixture for SnapshotsTable
    """
    yield snowflake_data_source.get_source_table(
        database_name="sf_database",
        schema_name="sf_schema",
        table_name="another_snapshots_table",
    )


@pytest.fixture(name="snowflake_feature_store_id")
def snowflake_feature_store_id_fixture():
    """Snowflake feature store id"""
    return ObjectId("646f6c190ed28a5271fb02a1")


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


@pytest.fixture(name="snowflake_event_table_with_timestamp_schema_id")
def snowflake_event_table_with_timestamp_schema_id_fixture():
    """Snowflake event table ID"""
    return ObjectId("67c68ea033e0a38cbf517412")


@pytest.fixture(name="snowflake_event_table_with_partition_column_id")
def snowflake_event_table_with_partition_column_id_fixture():
    """Snowflake event table ID with partition column schema"""
    return ObjectId("68649bb0371e74d37cd68f2e")


@pytest.fixture(name="snowflake_item_table_id")
def snowflake_item_table_id_fixture():
    """Snowflake event table ID"""
    return ObjectId("6337f9651050ee7d5980662d")


@pytest.fixture(name="snowflake_item_table_id_2")
def snowflake_item_table_id_2_fixture():
    """Snowflake event table ID"""
    return ObjectId("6337f9651050ee7d5980662e")


@pytest.fixture(name="snowflake_item_table_with_timestamp_schema_id")
def snowflake_item_table_with_timestamp_schema_id_fixture():
    """Snowflake item table ID with timestamp schema"""
    return ObjectId("67c9170033e0a38cbf517413")


@pytest.fixture(name="snowflake_time_series_table_id")
def snowflake_time_series_table_id_fixture():
    """Snowflake time series table ID"""
    return ObjectId("6337f9651050ee7d5980662f")


@pytest.fixture(name="snowflake_snapshots_table_id")
def snowflake_snapshots_table_id_fixture():
    """Snowflake snapshots table ID"""
    return ObjectId("6893ffbc6782e0c8fce7d072")


@pytest.fixture(name="another_snowflake_snapshots_table_id")
def another_snowflake_snapshots_table_id_fixture():
    """Snowflake snapshots table ID"""
    return ObjectId("68dd5a928c583807224804dc")


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


@pytest.fixture(name="gender_entity_id")
def gender_entity_id_fixture():
    """Gender entity ID"""
    return ObjectId("65f11f1d8a03610e41399306")


@pytest.fixture(name="snowflake_event_table")
def snowflake_event_table_fixture(
    snowflake_database_table,
    snowflake_event_table_id,
    catalog,
    mock_detect_and_update_column_dtypes,
):
    """EventTable object fixture"""
    _ = catalog, mock_detect_and_update_column_dtypes
    event_table = snowflake_database_table.create_event_table(
        name="sf_event_table",
        event_id_column="col_int",
        event_timestamp_column="event_timestamp",
        record_creation_timestamp_column="created_at",
        description="test event table",
        _id=snowflake_event_table_id,
    )
    assert event_table.frame.node.parameters.id == event_table.id
    assert event_table.id == snowflake_event_table_id
    yield event_table


@pytest.fixture(name="snowflake_event_table_with_tz_offset_column")
def snowflake_event_table_with_tz_offset_column_fixture(
    snowflake_database_table_no_tz,
    snowflake_event_table_with_tz_offset_column_id,
    transaction_entity,
    cust_id_entity,
    catalog,
    mock_detect_and_update_column_dtypes,
):
    """EventTable object fixture"""
    _ = catalog, mock_detect_and_update_column_dtypes
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
    catalog,
    mock_detect_and_update_column_dtypes,
):
    """EventTable object fixture"""
    _ = catalog, mock_detect_and_update_column_dtypes
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


@pytest.fixture(name="snowflake_event_table_with_timestamp_schema")
def snowflake_event_table_with_timestamp_schema_fixture(
    snowflake_database_table_no_tz,
    snowflake_event_table_with_timestamp_schema_id,
    transaction_entity,
    cust_id_entity,
    catalog,
    mock_detect_and_update_column_dtypes,
):
    """EventTable object fixture"""
    _ = catalog, mock_detect_and_update_column_dtypes
    event_table = snowflake_database_table_no_tz.create_event_table(
        name="sf_event_table",
        event_id_column="col_int",
        event_timestamp_column="event_timestamp",
        event_timestamp_timezone_offset_column="tz_offset",
        event_timestamp_schema=TimestampSchema(
            is_utc_time=False,
            timezone=TimeZoneColumn(column_name="tz_offset", type="offset"),
        ),
        record_creation_timestamp_column="created_at",
        _id=snowflake_event_table_with_timestamp_schema_id,
    )
    event_table["col_int"].as_entity(transaction_entity.name)
    event_table["cust_id"].as_entity(cust_id_entity.name)
    yield event_table


@pytest.fixture(name="snowflake_event_table_with_partition_column")
def snowflake_event_table_with_partition_column_fixture(
    snowflake_database_table,
    snowflake_event_table_with_partition_column_id,
    transaction_entity,
    cust_id_entity,
    catalog,
    mock_detect_and_update_column_dtypes,
):
    """EventTable object fixture"""
    _ = catalog, mock_detect_and_update_column_dtypes

    # mark a column as partition key
    snowflake_database_table.columns_info[0].partition_metadata = PartitionMetadata(
        is_partition_key=True,
    )

    event_table = snowflake_database_table.create_event_table(
        name="sf_event_table",
        event_id_column="col_int",
        event_timestamp_column="event_timestamp",
        record_creation_timestamp_column="created_at",
        description="Some description",
        datetime_partition_column="col_text",
        datetime_partition_schema=TimestampSchema(
            format_string="%Y-%m-%d %H:%M:%S",
        ),
        _id=snowflake_event_table_with_partition_column_id,
    )
    event_table["col_int"].as_entity(transaction_entity.name)
    event_table["cust_id"].as_entity(cust_id_entity.name)
    yield event_table


@pytest.fixture(name="snowflake_dimension_table")
def snowflake_dimension_table_fixture(
    snowflake_database_table_dimension_table,
    snowflake_dimension_table_id,
    catalog,
    mock_detect_and_update_column_dtypes,
):
    """DimensionTable object fixture"""
    _ = catalog, mock_detect_and_update_column_dtypes
    dimension_table = snowflake_database_table_dimension_table.create_dimension_table(
        name="sf_dimension_table",
        dimension_id_column="col_int",
        record_creation_timestamp_column="created_at",
        description="test dimension table",
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
        description="test scd table",
        _id=snowflake_scd_table_id,
    )
    assert scd_table.frame.node.parameters.id == scd_table.id
    yield scd_table


@pytest.fixture(name="snowflake_scd_table_state_map")
def snowflake_scd_table_state_map_fixture(snowflake_data_source):
    """SCDTable object fixture"""
    source_table = snowflake_data_source.get_source_table(
        database_name="sf_database",
        schema_name="sf_schema",
        table_name="scd_table_state_map",
    )
    scd_table = source_table.create_scd_table(
        name="scd_table_state_map",
        natural_key_column="col_text",
        surrogate_key_column="col_int",
        effective_timestamp_column="effective_timestamp",
        end_timestamp_column="end_timestamp",
        current_flag_column="is_active",
    )
    yield scd_table


@pytest.fixture(name="snowflake_scd_table_with_entity")
def snowflake_scd_table_with_entity_fixture(
    snowflake_scd_table, cust_id_entity, gender_entity, another_entity
):
    """
    Fixture for an SCD table with entity
    """
    snowflake_scd_table["col_text"].as_entity(cust_id_entity.name)  # natural key column
    snowflake_scd_table["col_boolean"].as_entity(gender_entity.name)
    snowflake_scd_table["col_binary"].as_entity(another_entity.name)
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
        description="test item table",
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
    catalog,
):
    """
    Snowflake ItemTable object fixture where the EventTable has a timezone offset column
    """
    _ = catalog
    _ = mock_get_persistent
    item_table = snowflake_database_table_item_table.create_item_table(
        name="sf_item_table",
        event_id_column="event_id_col",
        item_id_column="item_id_col",
        event_table_name=snowflake_event_table_with_tz_offset_column.name,
    )
    yield item_table


@pytest.fixture(name="snowflake_item_table_with_timestamp_schema")
def snowflake_item_table_with_timestamp_schema_fixture(
    snowflake_database_table_item_table,
    mock_get_persistent,
    snowflake_item_table_with_timestamp_schema_id,
    snowflake_event_table_with_timestamp_schema,
):
    """
    Snowflake ItemTable object fixture
    """
    _ = mock_get_persistent
    item_table = snowflake_database_table_item_table.create_item_table(
        name="sf_item_table",
        event_id_column="event_id_col",
        item_id_column="item_id_col",
        event_table_name=snowflake_event_table_with_timestamp_schema.name,
        description="test item table",
        _id=snowflake_item_table_with_timestamp_schema_id,
    )
    assert item_table.frame.node.parameters.id == item_table.id
    yield item_table


@pytest.fixture(name="snowflake_time_series_table")
def snowflake_time_series_table_fixture(
    snowflake_database_time_series_table,
    snowflake_time_series_table_id,
    catalog,
    mock_detect_and_update_column_dtypes,
):
    """TimeSeriesTable object fixture"""
    _ = catalog, mock_detect_and_update_column_dtypes
    time_series_table = snowflake_database_time_series_table.create_time_series_table(
        name="sf_time_series_table",
        series_id_column="col_int",
        reference_datetime_column="date",
        reference_datetime_schema=TimestampSchema(
            timezone="Etc/UTC", format_string="YYYY-MM-DD HH24:MI:SS"
        ),
        time_interval=TimeInterval(value=1, unit="DAY"),
        record_creation_timestamp_column="created_at",
        description="test time series table",
        _id=snowflake_time_series_table_id,
        datetime_partition_column="date",
        datetime_partition_schema=TimestampSchema(
            timezone="Etc/UTC", format_string="YYYY-MM-DD HH24:MI:SS"
        ),
    )
    assert time_series_table.frame.node.parameters.id == time_series_table.id
    assert time_series_table.id == snowflake_time_series_table_id
    yield time_series_table


@pytest.fixture(name="snowflake_snapshots_table")
def snowflake_snapshots_table_fixture(
    snowflake_database_snapshots_table,
    snowflake_snapshots_table_id,
    catalog,
    mock_detect_and_update_column_dtypes,
):
    """SnapshotsTable object fixture"""
    _ = catalog, mock_detect_and_update_column_dtypes
    snapshots_table = snowflake_database_snapshots_table.create_snapshots_table(
        name="sf_snapshots_table",
        snapshot_id_column="col_int",
        snapshot_datetime_column="date",
        snapshot_datetime_schema=TimestampSchema(
            timezone="Etc/UTC", format_string="YYYY-MM-DD HH24:MI:SS"
        ),
        time_interval=TimeInterval(value=1, unit="DAY"),
        record_creation_timestamp_column="created_at",
        description="test snapshots table",
        _id=snowflake_snapshots_table_id,
        datetime_partition_column="date",
        datetime_partition_schema=TimestampSchema(
            timezone="Etc/UTC", format_string="YYYY-MM-DD HH24:MI:SS"
        ),
    )
    assert snapshots_table.frame.node.parameters.id == snapshots_table.id
    assert snapshots_table.id == snowflake_snapshots_table_id
    yield snapshots_table


@pytest.fixture(name="another_snowflake_snapshots_table")
def another_snowflake_snapshots_table_fixture(
    another_snowflake_database_snapshots_table,
    another_snowflake_snapshots_table_id,
    catalog,
    mock_detect_and_update_column_dtypes,
):
    """SnapshotsTable object fixture"""
    _ = catalog, mock_detect_and_update_column_dtypes
    snapshots_table = another_snowflake_database_snapshots_table.create_snapshots_table(
        name="sf_snapshots_table_2",
        snapshot_id_column="col_int",
        snapshot_datetime_column="date",
        snapshot_datetime_schema=TimestampSchema(timezone="Etc/UTC", format_string="YYYY-MM-DD"),
        time_interval=TimeInterval(value=1, unit="DAY"),
        record_creation_timestamp_column="created_at",
        description="test snapshots table",
        _id=another_snowflake_snapshots_table_id,
        datetime_partition_column="date",
        datetime_partition_schema=TimestampSchema(timezone="Etc/UTC", format_string="YYYY-MM-DD"),
    )
    assert snapshots_table.frame.node.parameters.id == snapshots_table.id
    assert snapshots_table.id == another_snowflake_snapshots_table_id
    yield snapshots_table


@pytest.fixture(name="cust_id_entity")
def cust_id_entity_fixture(cust_id_entity_id, catalog):
    """
    Customer ID entity fixture
    """
    _ = catalog
    entity = Entity(name="customer", serving_names=["cust_id"], _id=cust_id_entity_id)
    entity.save()
    yield entity


@pytest.fixture(name="transaction_entity")
def transaction_entity_fixture(transaction_entity_id, catalog):
    """
    Event entity fixture
    """
    _ = catalog
    entity = Entity(name="transaction", serving_names=["transaction_id"], _id=transaction_entity_id)
    entity.save()
    assert entity.id == transaction_entity_id
    yield entity


@pytest.fixture(name="gender_entity")
def gender_entity_fixture(gender_entity_id, catalog):
    """
    Gender entity fixture
    """
    _ = catalog
    entity = Entity(name="gender", serving_names=["gender"], _id=gender_entity_id)
    entity.save()
    yield entity


@pytest.fixture(name="another_entity")
def another_entity_fixture(catalog):
    """
    Another entity fixture
    """
    _ = catalog
    entity = Entity(
        name="another", serving_names=["another_key"], _id=ObjectId("65b123107011cad326ada330")
    )
    entity.save()
    yield entity


@pytest.fixture(name="group_entity")
def group_entity_fixture(catalog):
    """
    Another entity to support creating test cases
    """
    _ = catalog
    entity = Entity(
        name="group", serving_names=["group_key"], _id=ObjectId("66334f9527378f612b42067a")
    )
    entity.save()
    yield entity


@pytest.fixture(name="item_entity")
def item_entity_fixture(catalog):
    """
    Item entity fixture
    """
    _ = catalog
    entity = Entity(
        name="item", serving_names=["item_id"], _id=ObjectId("664a3e617ac430c2ae37aede")
    )
    entity.save()
    yield entity


@pytest.fixture(name="item_type_entity")
def item_type_entity_fixture(catalog):
    """
    Item type entity fixture
    """
    _ = catalog
    entity = Entity(
        name="item_type", serving_names=["item_type"], _id=ObjectId("664a3e7d7ac430c2ae37aedf")
    )
    entity.save()
    yield entity


@pytest.fixture(name="snowflake_event_table_with_entity")
def snowflake_event_table_with_entity_fixture(
    snowflake_event_table,
    cust_id_entity,
    transaction_entity,
    mock_api_object_cache,
    mock_detect_and_update_column_dtypes,
    patch_initialize_entity_dtype,
):
    """
    Entity fixture that sets cust_id in snowflake_event_table as an Entity
    """
    _ = mock_api_object_cache, mock_detect_and_update_column_dtypes, patch_initialize_entity_dtype
    snowflake_event_table.cust_id.as_entity(cust_id_entity.name)
    snowflake_event_table.col_int.as_entity(transaction_entity.name)
    yield snowflake_event_table


@pytest.fixture(name="related_event_table_with_entity")
def related_event_table_with_entity_fixture(
    snowflake_database_table,
    snowflake_data_source,
    transaction_entity,
    another_entity,
    mock_api_object_cache,
    mock_detect_and_update_column_dtypes,
    patch_initialize_entity_dtype,
):
    """
    Fixture for an EventTable that is related to another EventTable
    """
    _ = mock_api_object_cache, mock_detect_and_update_column_dtypes, patch_initialize_entity_dtype
    with patch(
        "featurebyte.service.base_document.BaseDocumentService._check_document_unique_constraint"
    ):
        event_table = snowflake_database_table.create_event_table(
            name="related_event_table",
            event_id_column="col_int",
            event_timestamp_column="event_timestamp",
            record_creation_timestamp_column="created_at",
            description="Some description",
            datetime_partition_column="col_text",
            datetime_partition_schema=TimestampSchema(
                format_string="%Y-%m-%d %H:%M:%S",
            ),
            _id=ObjectId("6887258db73d336a2f65d021"),
        )
    event_table["col_int"].as_entity(transaction_entity.name)
    event_table["col_text"].as_entity(another_entity.name)
    return event_table


@pytest.fixture(name="snowflake_time_series_table_with_entity")
def snowflake_time_series_table_with_entity_fixture(
    snowflake_time_series_table,
    cust_id_entity,
    transaction_entity,
    mock_api_object_cache,
    mock_detect_and_update_column_dtypes,
    patch_initialize_entity_dtype,
):
    """
    Entity fixture that sets cust_id in snowflake_time_series_table as an Entity
    """
    _ = mock_api_object_cache, mock_detect_and_update_column_dtypes, patch_initialize_entity_dtype
    snowflake_time_series_table.store_id.as_entity(cust_id_entity.name)
    snowflake_time_series_table.col_int.as_entity(transaction_entity.name)
    yield snowflake_time_series_table


@pytest.fixture(name="snowflake_snapshots_table_with_entity")
def snowflake_snapshots_table_with_entity_fixture(
    snowflake_snapshots_table,
    cust_id_entity,
    transaction_entity,
    another_entity,
    mock_api_object_cache,
    mock_detect_and_update_column_dtypes,
    patch_initialize_entity_dtype,
):
    """
    Entity fixture that sets cust_id in snowflake_snapshots_table as an Entity
    """
    _ = mock_api_object_cache, mock_detect_and_update_column_dtypes, patch_initialize_entity_dtype
    snowflake_snapshots_table.store_id.as_entity(cust_id_entity.name)
    snowflake_snapshots_table.col_int.as_entity(transaction_entity.name)
    snowflake_snapshots_table.col_binary.as_entity(another_entity.name)
    yield snowflake_snapshots_table


@pytest.fixture(name="arbitrary_default_feature_job_setting")
def arbitrary_default_feature_job_setting_fixture():
    """
    Get arbitrary default feature job setting
    """
    return FeatureJobSetting(blind_spot="1m30s", period="15m", offset="3m")


@pytest.fixture(name="arbitrary_default_cron_feature_job_setting")
def arbitrary_default_cron_feature_job_setting_fixture():
    """
    Get arbitrary default feature job setting
    """
    return CronFeatureJobSetting(
        crontab=Crontab(
            minute=0,
            hour=1,
            day_of_week="*",
            day_of_month="*",
            month_of_year="*",
        ),
        timezone="Etc/UTC",
    )


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
            "database_name": "sf_database",
            "schema_name": "sf_schema",
            "warehouse": "sf_warehouse",
            "role_name": "TESTING",
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


@pytest.fixture(name="snowflake_item_view_with_entity")
def snowflake_item_view_with_entity_fixture(
    snowflake_item_table, transaction_entity, item_entity, item_type_entity
):
    """
    Snowflake item view with entity
    """
    snowflake_item_table["event_id_col"].as_entity(transaction_entity.name)
    snowflake_item_table["item_id_col"].as_entity(item_entity.name)
    snowflake_item_table["item_type"].as_entity(item_type_entity.name)
    item_view = snowflake_item_table.get_view(event_suffix="_event")
    return item_view


@pytest.fixture(name="snowflake_scd_view_with_entity")
def snowflake_scd_view_with_entity_fixture(snowflake_scd_table_with_entity):
    """
    Fixture for an SCD view with entity
    """
    return snowflake_scd_table_with_entity.get_view()


@pytest.fixture(name="snowflake_time_series_view_with_entity")
def snowflake_time_series_view_with_entity_fixture(snowflake_time_series_table_with_entity):
    """
    Snowflake time series view with entity
    """
    return snowflake_time_series_table_with_entity.get_view()


@pytest.fixture(name="freeze_time_observation_table_task")
def freeze_time_observation_table_task_fixture():
    """
    Freeze time for ObservationTableTask due to freezegun not working well with pydantic in some
    cases (in this case, apparently only the ObservationTableTask)
    """
    with safe_freeze_time("2011-03-08T15:37:00"):
        yield


@pytest.fixture(name="patch_observation_table_task_get_table_with_missing_data", autouse=True)
def patch_observation_table_task_get_table_with_missing_data_fixture():
    """Patch ObservationTableTask.get_table_with_missing_data to return None"""
    with (
        patch(
            "featurebyte.worker.task.observation_table.ObservationTableTask.get_table_with_missing_data"
        ) as mock_observation_table_task_get_table_with_missing_data,
        patch(
            "featurebyte.worker.task.target_table.TargetTableTask.get_table_with_missing_data"
        ) as mock_target_table_task_get_table_with_missing_data,
    ):
        mock_observation_table_task_get_table_with_missing_data.return_value = None
        mock_target_table_task_get_table_with_missing_data.return_value = None
        yield


@pytest.fixture(name="patched_observation_table_service")
def patched_observation_table_service_fixture(freeze_time_observation_table_task):
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
            "num_rows": 100,
            "most_recent_point_in_time": "2023-01-15 10:00:00",
        }

    with patch(
        "featurebyte.service.observation_table.ObservationTableService.validate_materialized_table_and_get_metadata",
        Mock(side_effect=mocked_get_additional_metadata),
    ):
        yield


@pytest.fixture(name="patched_observation_table_service_for_preview")
def patched_observation_table_service_for_preview_fixture():
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
            "num_rows": 50,
            "most_recent_point_in_time": "2023-01-15 10:00:00",
        }

    with patch(
        "featurebyte.service.observation_table.ObservationTableService.validate_materialized_table_and_get_metadata",
        Mock(side_effect=mocked_get_additional_metadata),
    ):
        yield


@pytest.fixture(name="patched_static_source_table_service")
def patched_static_source_table_service_fixture():
    """
    Patch StaticSourceTableService.validate_materialized_table_and_get_metadata
    """

    async def mocked_get_additional_metadata(*args, **kwargs):
        _ = args
        _ = kwargs
        return {
            "columns_info": [
                {"name": "cust_id", "dtype": "INT"},
                {"name": "timestamp", "dtype": "TIMESTAMP"},
            ],
            "num_rows": 100,
        }

    with patch(
        "featurebyte.service.static_source_table.StaticSourceTableService.validate_materialized_table_and_get_metadata",
        Mock(side_effect=mocked_get_additional_metadata),
    ):
        yield


@pytest.fixture(name="snowflake_execute_query_invalid_batch_request_table")
def snowflake_execute_query_invalid_batch_request_table(snowflake_connector, snowflake_query_map):
    """
    Fixture to patch SnowflakeSession.execute_query to return invalid shcema for batch request table
    creation task (missing a required entity column)
    """
    _ = snowflake_connector

    def side_effect(
        query, timeout=DEFAULT_EXECUTE_QUERY_TIMEOUT_SECONDS, to_log_error=True, query_metadata=None
    ):
        _ = timeout, to_log_error, query_metadata
        # By not handling the SHOW COLUMNS query specifically, the schema will be empty and
        # missing a required entity column "cust_id"
        if "COUNT(*)" in query:
            res = [
                {
                    "row_count": 500,
                }
            ]
        else:
            res = snowflake_query_map.get(query)

        if res is not None:
            return pd.DataFrame(res)
        return None

    with mock.patch(
        "featurebyte.session.snowflake.SnowflakeSession.execute_query"
    ) as mock_execute_query:
        mock_execute_query.side_effect = side_effect
        yield mock_execute_query


@pytest.fixture(name="snowflake_execute_query_for_materialized_table")
def snowflake_execute_query_for_materialized_table_fixture(
    snowflake_connector,
    snowflake_query_map,
):
    """
    Extended version of the default execute_query mock to handle more queries expected when running
    materialized table creation tasks.
    """
    _ = snowflake_connector

    def side_effect(
        query, timeout=DEFAULT_EXECUTE_QUERY_TIMEOUT_SECONDS, to_log_error=True, query_metadata=None
    ):
        _ = timeout, to_log_error, query_metadata
        if query.startswith('SHOW COLUMNS IN "sf_database"."sf_schema"."BATCH_FEATURE_TABLE'):
            res = [
                {
                    "column_name": "cust_id",
                    "data_type": json.dumps({"type": "FIXED", "scale": 0}),
                    "comment": None,
                },
                {
                    "column_name": "POINT_IN_TIME",
                    "data_type": json.dumps({"type": "TIMESTAMP_NTZ", "scale": 0}),
                    "comment": None,
                },
                {
                    "column_name": "sum_30m",
                    "data_type": json.dumps({"type": "float", "scale": 0}),
                    "comment": None,
                },
            ]
        elif query.startswith('SHOW COLUMNS IN "sf_database"."sf_schema"'):
            res = [
                {
                    "column_name": "cust_id",
                    "data_type": json.dumps({"type": "FIXED", "scale": 0}),
                    "comment": None,
                },
                {
                    "column_name": "POINT_IN_TIME",
                    "data_type": json.dumps({"type": "TIMESTAMP_NTZ", "scale": 0}),
                    "comment": None,
                },
                {
                    "column_name": "target",
                    "data_type": json.dumps({"type": "float", "scale": 0}),
                    "comment": None,
                },
            ]
        elif "is_row_index_valid" in query:
            return pd.DataFrame({"is_row_index_valid": [True]})
        elif "COUNT(*)" in query:
            res = [
                {
                    "row_count": 500,
                }
            ]
        elif query.startswith("SELECT") and "new_batch_prediction_table" in query:
            raise ProgrammingError()
        else:
            res = snowflake_query_map.get(query)
        if res is not None:
            return pd.DataFrame(res)
        return None

    with mock.patch(
        "featurebyte.session.snowflake.SnowflakeSession.execute_query"
    ) as mock_execute_query:
        mock_execute_query.side_effect = side_effect
        yield mock_execute_query


@pytest.fixture(name="snowflake_execute_query_for_observation_table")
def snowflake_execute_query_for_observation_table_fixture(
    snowflake_connector,
    snowflake_query_map,
):
    """
    Extended version of the default execute_query mock to handle more queries expected when running
    observation table creation tasks.
    """
    _ = snowflake_connector

    def side_effect(
        query, timeout=DEFAULT_EXECUTE_QUERY_TIMEOUT_SECONDS, to_log_error=True, query_metadata=None
    ):
        _ = timeout, to_log_error, query_metadata
        if "COUNT(*)" in query:
            res = [
                {
                    "row_count": 500,
                }
            ]
        else:
            res = snowflake_query_map.get(query)
        if res is not None:
            return pd.DataFrame(res)
        return None

    with mock.patch(
        "featurebyte.session.snowflake.SnowflakeSession.execute_query"
    ) as mock_execute_query:
        mock_execute_query.side_effect = side_effect
        yield mock_execute_query


@pytest.fixture(name="observation_table_from_source")
def observation_table_from_source_fixture(
    snowflake_database_table,
    patched_observation_table_service,
    snowflake_execute_query_for_observation_table,
    catalog,
    context,
):
    """
    Observation table created from SourceTable
    """
    _ = catalog
    _ = patched_observation_table_service
    return snowflake_database_table.create_observation_table(
        "observation_table_from_source_table",
        context_name=context.name,
        columns_rename_mapping={"event_timestamp": "POINT_IN_TIME"},
    )


@pytest.fixture(name="observation_table_from_view")
def observation_table_from_view_fixture(
    snowflake_event_view_with_entity, patched_observation_table_service
):
    """
    Observation table created from EventView
    """
    _ = patched_observation_table_service
    return snowflake_event_view_with_entity.create_observation_table(
        "observation_table_from_event_view",
        columns_rename_mapping={"col_int": "transaction_id", "event_timestamp": "POINT_IN_TIME"},
    )


@pytest.fixture(name="static_source_table_from_source")
def static_source_table_from_source_fixture(
    snowflake_database_table, patched_static_source_table_service, catalog
):
    """
    Static source table created from SourceTable
    """
    _ = catalog
    _ = patched_static_source_table_service
    return snowflake_database_table.create_static_source_table(
        "static_source_table_from_source_table"
    )


@pytest.fixture(name="static_source_table_from_view")
def static_source_table_from_view_fixture(
    snowflake_event_view, patched_static_source_table_service
):
    """
    Static source table created from EventView
    """
    _ = patched_static_source_table_service
    return snowflake_event_view.create_static_source_table("static_source_table_from_event_view")


@pytest.fixture(name="historical_feature_table")
def historical_feature_table_fixture(
    float_feature,
    observation_table_from_source,
    snowflake_execute_query_for_materialized_table,
    mocked_compute_tiles_on_demand,
):
    """
    Fixture for a HistoricalFeatureTable
    """
    _ = snowflake_execute_query_for_materialized_table
    _ = mocked_compute_tiles_on_demand
    feature_list = FeatureList([float_feature], name="feature_list_for_historical_feature_table")
    feature_list.save()
    historical_feature_table = feature_list.compute_historical_feature_table(
        observation_table_from_source, "my_historical_feature_table"
    )
    return historical_feature_table


@pytest.fixture(name="target_table")
def target_table_fixture(
    float_target, observation_table_from_source, snowflake_execute_query_for_materialized_table
):
    """
    Fixture for a TargetTable
    """
    _ = snowflake_execute_query_for_materialized_table
    if not float_target.saved:
        float_target.save()
    return float_target.compute_target_table(observation_table_from_source, "my_target_table")


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
    return FeatureJobSetting(blind_spot="10m", period="30m", offset="5m")


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


@pytest.fixture(name="context")
def context_fixture(cust_id_entity):
    """
    Context fixture
    """
    context = Context(name="context", primary_entity_ids=[cust_id_entity.id])
    if not context.saved:
        context.save()
    return context


@pytest.fixture(name="float_target")
def float_target_fixture(grouped_event_view):
    """
    Float target fixture
    """
    target = grouped_event_view.forward_aggregate(
        method="sum",
        value_column="col_float",
        window="1d",
        target_name="float_target",
        fill_value=0.0,
    )
    return target


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


@pytest.fixture(name="non_time_based_features")
def get_non_time_based_features_fixture(snowflake_item_table, transaction_entity):
    """
    Get a non-time-based feature.

    This is a non-time-based feature as it is built from ItemTable.
    """
    snowflake_item_table.event_id_col.as_entity(transaction_entity.name)
    item_table = ItemTable(**{**snowflake_item_table.json_dict(), "item_id_column": "item_id_col"})
    item_view = item_table.get_view(event_suffix="_event_table")
    feature_1 = item_view.groupby("event_id_col").aggregate(
        value_column="item_amount",
        method=AggFunc.SUM,
        feature_name="non_time_time_sum_amount_feature",
    )
    feature_2 = item_view.groupby("event_id_col").aggregate(
        value_column="item_amount",
        method=AggFunc.MAX,
        feature_name="non_time_time_max_amount_feature",
    )
    return [feature_1, feature_2]


@pytest.fixture(name="non_time_based_feature")
def get_non_time_based_feature_fixture(non_time_based_features):
    """
    Get a non-time-based feature.
    """
    return non_time_based_features[0]


@pytest.fixture(name="filtered_non_time_based_feature")
def filtered_non_time_based_feature_fixture(snowflake_item_table, transaction_entity):
    """
    Get a non-time-based feature that is from a filtered ItemView
    """
    snowflake_item_table.event_id_col.as_entity(transaction_entity.name)
    item_table = ItemTable(**{**snowflake_item_table.json_dict(), "item_id_column": "item_id_col"})
    item_view = item_table.get_view(event_suffix="_event_table")
    item_view = item_view[item_view["item_amount"] > 10]
    feature = item_view.groupby("event_id_col").aggregate(
        value_column="item_amount",
        method=AggFunc.SUM,
        feature_name="non_time_time_sum_amount_feature_gt10",
    )
    feature.save()
    return feature


@pytest.fixture(name="non_time_based_feature_with_event_timestamp_schema")
def non_time_based_feature_with_event_timestamp_schema(
    snowflake_item_table_with_timestamp_schema, transaction_entity
):
    """
    Get a non-time-based feature with event timestamp schema
    """
    snowflake_item_table_with_timestamp_schema.event_id_col.as_entity(transaction_entity.name)
    item_table = ItemTable(**{
        **snowflake_item_table_with_timestamp_schema.json_dict(),
        "item_id_column": "item_id_col",
    })
    item_view = item_table.get_view(event_suffix="_event_table")
    feature = item_view.groupby("event_id_col").aggregate(
        value_column="item_amount",
        method=AggFunc.SUM,
        feature_name="non_time_time_sum_amount_feature",
    )
    feature.save()
    return feature


@pytest.fixture(name="float_feature_with_event_timestamp_schema")
def float_feature_with_event_timestamp_schema_fixture(
    snowflake_event_table_with_timestamp_schema, cust_id_entity, feature_group_feature_job_setting
):
    """
    Get a non-time-based feature with event timestamp schema
    """
    snowflake_event_table_with_timestamp_schema.cust_id.as_entity(cust_id_entity.name)
    event_view = snowflake_event_table_with_timestamp_schema.get_view()
    feature = event_view.groupby("cust_id").aggregate_over(
        value_column="col_int",
        method="sum",
        windows=["30m"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=["sum_30m"],
    )["sum_30m"]
    feature = feature.astype(float)
    feature.name = "sum_30m"
    feature.save()
    return feature


@pytest.fixture(name="float_feature_with_partition_column")
def float_feature_with_partition_column_fixture(
    snowflake_event_table_with_partition_column, cust_id_entity, feature_group_feature_job_setting
):
    """
    Get a non-time-based feature with event timestamp schema
    """
    snowflake_event_table_with_partition_column.cust_id.as_entity(cust_id_entity.name)
    event_view = snowflake_event_table_with_partition_column.get_view()
    feature = event_view.groupby("cust_id").aggregate_over(
        value_column="col_int",
        method="sum",
        windows=["30m"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=["sum_30m"],
    )["sum_30m"]
    feature = feature.astype(float)
    feature.name = "sum_30m"
    feature.save()
    return feature


@pytest.fixture(name="float_feature")
def float_feature_fixture(feature_group):
    """
    Float Feature fixture
    """
    feature = feature_group["sum_1d"]
    assert isinstance(feature, Feature)
    assert feature_group["sum_1d"].table_ids == feature.table_ids
    global_graph = GlobalQueryGraph()
    assert id(feature.graph.nodes) == id(global_graph.nodes)
    yield feature


@pytest.fixture(name="float_feature_different_job_setting")
def float_feature_different_job_setting_fixture(snowflake_event_view_with_entity):
    """
    Feature with different feature job setting
    """
    return snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["24h"],
        feature_job_setting=FeatureJobSetting(period="3h", blind_spot="15m", offset="5s"),
        feature_names=["sum_24h_every_3h"],
    )["sum_24h_every_3h"]


@pytest.fixture(name="float_feature_composite_entity")
def float_feature_composite_entity_fixture(
    snowflake_event_table_with_entity,
    another_entity,
    feature_group_feature_job_setting,
):
    """
    Feature with composite entity
    """
    snowflake_event_table_with_entity.col_text.as_entity(another_entity.name)
    event_view = snowflake_event_table_with_entity.get_view()
    feature = event_view.groupby(["cust_id", "col_text"]).aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["1d"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=["composite_entity_feature_1d"],
    )["composite_entity_feature_1d"]
    yield feature


@pytest.fixture(name="float_feature_composite_entity_v2")
def float_feature_composite_entity_v2_fixture(
    float_feature_composite_entity,
):
    """
    Another feature with composite entity
    """
    feature = float_feature_composite_entity + 123
    feature.name = float_feature_composite_entity.name + "_plus_123"
    return feature


@pytest.fixture(name="float_feature_multiple_windows")
def float_feature_multiple_windows_fixture(feature_group):
    """
    Float Feature fixture
    """
    feature = feature_group["sum_2h"] / feature_group["sum_1d"]
    feature.name = "sum_ratio_2h_over_1d"
    # Save and retrieve so that the graph is pruned
    feature.save()
    yield Feature.get_by_id(feature.id)


@pytest.fixture(name="bool_feature")
def bool_feature_fixture(float_feature):
    """
    Boolean Feature fixture
    """
    bool_feature = float_feature > 100.0
    assert isinstance(bool_feature, Feature)
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
        feature_job_setting=FeatureJobSetting(blind_spot="10m", period="30m", offset="5m"),
        feature_names=["sum_30m_by_category", "sum_2h_by_category", "sum_1d_by_category"],
    )
    yield features["sum_1d_by_category"]


@pytest.fixture(name="count_per_category_feature_group")
def count_per_category_feature_group_fixture(snowflake_event_view_with_entity):
    """
    Aggregation per category FeatureGroup fixture
    """
    grouped = snowflake_event_view_with_entity.groupby("cust_id", category="col_int")
    features = grouped.aggregate_over(
        value_column=None,
        method="count",
        windows=["30m", "2h", "1d"],
        feature_job_setting=FeatureJobSetting(blind_spot="10m", period="30m", offset="5m"),
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
        feature_job_setting=FeatureJobSetting(blind_spot="10m", period="30m", offset="5m"),
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


@pytest.fixture(name="multiple_scd_joined_feature")
def multiple_scd_joined_feature_fixture(
    snowflake_event_table_with_entity,
    snowflake_scd_table,
    snowflake_scd_table_state_map,
):
    """
    Feature that is built from multiple SCD tables joined with event table
    """
    col_boolean_entity = Entity.create("col_boolean", serving_names=["col_boolean"])
    snowflake_scd_table.col_boolean.as_entity(col_boolean_entity.name)

    state_code_entity = Entity.create("state_code", serving_names=["state_code"])
    snowflake_scd_table_state_map.col_boolean.as_entity(col_boolean_entity.name)
    snowflake_scd_table_state_map.state_code.as_entity(state_code_entity.name)

    event_view = snowflake_event_table_with_entity.get_view()
    scd_view = snowflake_scd_table.get_view()
    state_view = snowflake_scd_table_state_map.get_view()
    event_view_cols = ["col_int", "event_timestamp"]
    event_view = event_view[event_view_cols].join(
        scd_view[["col_int", "col_boolean"]], on="col_int"
    )
    event_view = event_view.join(
        state_view[["col_boolean", "state_code"]], on="col_boolean", rsuffix="_scd"
    )

    feature = event_view.groupby("state_code_scd").aggregate_over(
        None,
        "count",
        windows=["30d"],
        feature_names=["state_code_counts_30d"],
        feature_job_setting=FeatureJobSetting(period="24h", offset="1h", blind_spot="2h"),
    )["state_code_counts_30d"]
    yield feature


@pytest.fixture(name="feature_without_entity")
def feature_without_entity_fixture(snowflake_event_table):
    """
    Fixture to get a feature without entity
    """
    event_view = snowflake_event_table.get_view()
    feature_group = event_view.groupby([]).aggregate_over(
        value_column=None,
        method="count",
        windows=["1d"],
        feature_job_setting=FeatureJobSetting(period="24h", offset="1h", blind_spot="2h"),
        feature_names=["count_1d"],
    )
    yield feature_group["count_1d"]


@pytest.fixture(name="scd_lookup_feature")
def scd_lookup_feature_fixture(snowflake_scd_table_with_entity):
    """
    Fixture to get a lookup feature from SCD table
    """
    scd_view = snowflake_scd_table_with_entity.get_view()
    feature = scd_view["col_boolean"].as_feature("some_lookup_feature")
    return feature


@pytest.fixture(name="aggregate_asat_feature")
def aggregate_asat_feature_fixture(snowflake_scd_table_with_entity):
    """
    Fixture to get an aggregate asat feature from SCD table
    """
    scd_view = snowflake_scd_table_with_entity.get_view()
    feature = scd_view.groupby("col_boolean").aggregate_asat(
        value_column=None,
        method="count",
        feature_name="asat_gender_count",
    )
    return feature


@pytest.fixture(name="aggregate_asat_no_entity_feature")
def aggregate_asat_no_entity_feature_fixture(snowflake_scd_table_with_entity):
    """
    Fixture to get an aggregate asat feature from SCD table without entity
    """
    scd_view = snowflake_scd_table_with_entity.get_view()
    feature = scd_view.groupby([]).aggregate_asat(
        value_column=None,
        method="count",
        feature_name="asat_overall_count",
    )
    return feature


@pytest.fixture(name="aggregate_asat_composite_entity_feature")
def aggregate_asat_composite_entity_fixture(snowflake_scd_table_with_entity):
    """
    Fixture to get an aggregate asat feature with composite entities from SCD table
    """
    scd_view = snowflake_scd_table_with_entity.get_view()
    feature = scd_view.groupby(["col_boolean", "col_binary"]).aggregate_asat(
        value_column=None,
        method="count",
        feature_name="asat_gender_x_other_count",
    )
    return feature


@pytest.fixture(name="item_view_window_aggregate_feature")
def item_view_window_aggregate_feature_fixture(snowflake_item_view_with_entity):
    """
    Fixture to get a window aggregate feature from an item view
    """
    return snowflake_item_view_with_entity.groupby(["item_type"]).aggregate_over(
        value_column="item_amount",
        method="sum",
        windows=["1d"],
        feature_names=["sum_1d"],
        feature_job_setting=FeatureJobSetting(blind_spot="10m", period="30m", offset="5m"),
    )["sum_1d"]


@pytest.fixture(name="descendant_of_gender_feature")
def descendant_of_gender_feature(snowflake_dimension_table, group_entity, gender_entity):
    """
    Fixture that has a primary entity that is the descendant of gender entity
    """
    # Create parent child relationship between group (child) and gender (parent)
    snowflake_dimension_table["col_int"].as_entity(group_entity.name)  # dimension id
    snowflake_dimension_table["col_boolean"].as_entity(gender_entity.name)
    view = snowflake_dimension_table.get_view()
    feature = view["col_float"].as_feature("descendant_of_gender_feature")
    return feature


@pytest.fixture(name="feature_with_internal_parent_child_relationships")
def feature_with_internal_parent_child_relationships_fixture(
    scd_lookup_feature, aggregate_asat_feature
):
    """
    Feature with internal parent child relationships, for example:

    C = A + B

    where B is a parent of A
    """
    feature = scd_lookup_feature.astype(str) + "_" + aggregate_asat_feature.astype(str)
    feature.name = "complex_parent_child_feature"
    return feature


@pytest.fixture(name="latest_event_timestamp_feature")
def latest_event_timestamp_feature_fixture(
    snowflake_event_view_with_entity, feature_group_feature_job_setting
):
    """
    Fixture for a timestamp feature
    """
    feature = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
        value_column="event_timestamp",
        method="latest",
        windows=["90d"],
        feature_names=["latest_event_timestamp_90d"],
        feature_job_setting=feature_group_feature_job_setting,
    )["latest_event_timestamp_90d"]
    return feature


@pytest.fixture(name="latest_event_timestamp_overall_feature")
def latest_event_timestamp_overall_feature_fixture(
    snowflake_event_view_with_entity, feature_group_feature_job_setting
):
    """
    Fixture for a timestamp feature
    """
    feature = snowflake_event_view_with_entity.groupby([]).aggregate_over(
        value_column="event_timestamp",
        method="latest",
        windows=["90d"],
        feature_names=["latest_event_timestamp_overall_90d"],
        feature_job_setting=feature_group_feature_job_setting,
    )["latest_event_timestamp_overall_90d"]
    return feature


@pytest.fixture(name="latest_event_timestamp_unbounded_feature")
def latest_event_timestamp_unbounded_feature_fixture(
    snowflake_event_view_with_entity, feature_group_feature_job_setting
):
    """
    Fixture for a latest aggregation feature without a window
    """
    feature = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
        value_column="event_timestamp",
        method="latest",
        windows=[None],
        feature_names=["latest_event_timestamp"],
        feature_job_setting=feature_group_feature_job_setting,
    )["latest_event_timestamp"]
    return feature


@pytest.fixture(name="count_distinct_window_aggregate_feature")
def count_distinct_window_aggregate_feature_fixture(
    snowflake_event_view_with_entity, feature_group_feature_job_setting
):
    """
    Fixture for a feature using count distinct aggregation
    """
    feature = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
        value_column="col_int",
        method="count_distinct",
        windows=["48h"],
        feature_names=["col_int_distinct_count_48h"],
        feature_job_setting=feature_group_feature_job_setting,
    )["col_int_distinct_count_48h"]
    return feature


@pytest.fixture(name="ts_window_aggregate_feature")
def ts_window_aggregate_feature_fixture(snowflake_time_series_view_with_entity):
    """
    Fixture for a feature using time series window aggregate
    """
    feature = snowflake_time_series_view_with_entity.groupby("store_id").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=[CalendarWindow(unit="MONTH", size=3)],
        feature_names=["col_float_sum_3month"],
        feature_job_setting=CronFeatureJobSetting(
            crontab="0 8 1 * *",
            blind_spot=CalendarWindow(unit="MONTH", size=1),
        ),
    )["col_float_sum_3month"]
    return feature


@pytest.fixture(name="ts_window_aggregate_feature_from_event_table")
def ts_window_aggregate_feature_from_event_table_fixture(snowflake_event_view_with_entity):
    """
    Fixture for a time series aggregate feature derived from an EventTable
    """
    feature = snowflake_event_view_with_entity.groupby(by_keys="cust_id").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=[CalendarWindow(unit="MONTH", size=3)],
        feature_names=["sum_3month"],
        feature_job_setting=CronFeatureJobSetting(
            crontab="0 8 1 * *",
        ),
    )["sum_3month"]
    return feature


@pytest.fixture(name="snapshots_lookup_feature")
def snapshots_lookup_feature_fixture(snowflake_snapshots_table_with_entity):
    """
    Fixture to get a lookup feature from Snapshots table
    """
    snapshots_view = snowflake_snapshots_table_with_entity.get_view()
    feature = snapshots_view["col_float"].as_feature("snapshots_lookup_feature")
    return feature


@pytest.fixture(name="snapshots_aggregate_asat_feature")
def snapshots_aggregate_asat_feature_fixture(snowflake_snapshots_table_with_entity):
    """
    Fixture to get an aggregate asat feature from Snapshots table
    """
    snapshots_view = snowflake_snapshots_table_with_entity.get_view()
    feature = snapshots_view.groupby("col_binary").aggregate_asat(
        value_column="col_float",
        method="sum",
        feature_name="snapshots_asat_col_int_sum",
    )
    return feature


@pytest.fixture(name="request_column_point_in_time")
def request_column_point_in_time():
    """
    Fixture for a RequestColumn object for the point in time
    """
    return RequestColumn.point_in_time()


@pytest.fixture(name="mock_snowflake_session")
def mock_snowflake_session_fixture():
    """
    SnowflakeSession object fixture
    """
    session = Mock(
        name="mock_snowflake_session",
        spec=SnowflakeSession,
        source_type=SourceType.SNOWFLAKE,
        database_name="sf_db",
        schema_name="sf_schema",
        _no_schema_error=ProgrammingError,
    )
    session.clone_if_not_threadsafe.return_value = session
    session.create_table_as.side_effect = partial(SnowflakeSession.create_table_as, session)
    session.table_exists.side_effect = partial(SnowflakeSession.table_exists, session)
    session.retry_sql.side_effect = partial(SnowflakeSession.retry_sql, session)
    session.list_table_schema.return_value = {}
    session.get_source_info.return_value = SourceInfo(
        database_name="sf_db", schema_name="sf_schema", source_type=SourceType.SNOWFLAKE
    )
    session.drop_tables.side_effect = partial(SnowflakeSession.drop_tables, session)
    type(session).adapter = PropertyMock(
        side_effect=lambda: get_sql_adapter(session.get_source_info())
    )
    return session


@pytest.fixture
def mock_snowflake_tile(snowflake_feature_store_id):
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
        feature_store_id=snowflake_feature_store_id,
        windows=["1d"],
    )

    return tile_spec


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
        feature_job_setting=FeatureJobSetting(blind_spot="10m", period="30m", offset="5m"),
    )
    feature = feature_group["sum_30m"]
    feature.__dict__["online_enabled"] = False
    return feature


@pytest.fixture(name="online_store_table_version_service")
def online_store_table_version_service_fixture(app_container):
    """
    OnlineStoreTableVersionService fixture
    """
    return app_container.online_store_table_version_service


@pytest.fixture(name="mocked_compute_tiles_on_demand")
def mocked_compute_tiles_on_demand():
    """Fixture for a mocked SnowflakeTileCache object"""
    with mock.patch(
        "featurebyte.service.historical_features_and_target.compute_tiles_on_demand",
        return_value=OnDemandTileComputeResult(
            tile_compute_metrics=TileComputeMetrics(),
            on_demand_tile_tables=[],
        ),
    ) as mocked_compute_tiles_on_demand:
        yield mocked_compute_tiles_on_demand


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
        "feature_job_setting_analysis",
        "context",
    ]
    output = {}
    for obj_name in object_names:
        filename = f"{base_path}/{obj_name}.json"
        with open(filename, "r") as fhandle:
            output[obj_name] = json.load(fhandle)["_id"]
    return output


@pytest.fixture(name="user_id", scope="session")
def user_id() -> ObjectId:
    """
    User ID fixture
    """
    return ObjectId("63f9506dd478b94127123456")


@pytest.fixture(scope="session")
def user(user_id) -> User:
    """
    Mock user
    """
    user = User()
    user.id = user_id
    return user


@pytest.fixture(name="catalog_id", scope="session")
def catalog_id_fixture() -> ObjectId:
    """
    User ID fixture
    """
    return ObjectId("63f9506dd478b94127123480")


@pytest.fixture(name="patched_catalog_get_create_payload")
def patched_catalog_get_create_payload_fixture(catalog_id, snowflake_feature_store):
    """
    Patch catalog get create payload
    """
    with mock.patch(
        "featurebyte.api.catalog.Catalog._get_create_payload"
    ) as mock_get_create_payload:
        mock_get_create_payload.return_value = CatalogCreate(
            _id=catalog_id,
            name="catalog",
            default_feature_store_ids=[snowflake_feature_store.id],
        ).json_dict()
        yield


@pytest.fixture(name="catalog")
def catalog_fixture(snowflake_feature_store):
    """
    Catalog fixture
    """
    return Catalog.create(name="catalog", feature_store_name=snowflake_feature_store.name)


@pytest.fixture(name="app_container_factory")
def app_container_factory_fixture(persistent, user, storage, temp_storage):
    """
    Return a function that creates an app container with a specific catalog_id.
    This allows tests to create multiple app containers with different catalog contexts.
    """

    def create_app_container(catalog_id):
        return LazyAppContainer(
            app_container_config=app_container_config,
            instance_map={
                "user": user,
                "persistent": persistent,
                "temp_storage": temp_storage,
                "celery": get_celery(),
                "storage": storage,
                "catalog_id": catalog_id,
                "task_id": uuid4(),
                "progress": AsyncMock(),
                "redis_uri": TEST_REDIS_URI,
            },
        )

    return create_app_container


@pytest.fixture(name="app_container_no_catalog")
def app_container_no_catalog_fixture(app_container_factory):
    """
    AppContainer fixture without catalog
    """
    return app_container_factory(catalog_id=None)


@pytest.fixture(name="app_container")
def app_container_fixture(app_container_factory, catalog):
    """
    Return an app container used in tests. This will allow us to easily retrieve instances of the right type.

    Note that this fixture should be initialized individually per test that is run as the instance map scope is mutable
    and can be over-ridden for each test to inject in specific dependencies that will be specific to the test. This
    means that we should not put a scope="session" on this fixture, and it is likely that test will fail if that
    change is made.
    """
    return app_container_factory(catalog_id=catalog.id)


@pytest_asyncio.fixture(name="insert_credential")
async def insert_credential_fixture(persistent, user, snowflake_feature_store_id):
    """
    Calling this fixture will insert the credential into the database.
    """
    credential_model = CredentialModel(
        name="sf_featurestore",
        feature_store_id=snowflake_feature_store_id,
        database_credential=UsernamePasswordCredential(
            username="sf_user",
            password="sf_password",
        ),
        user_id=user.id,
    )
    credential_model.encrypt_credentials()
    await persistent.insert_one(
        collection_name=CredentialModel.collection_name(),
        document=credential_model.model_dump(by_alias=True),
        user_id=user.id,
    )


TEST_TASK_REGISTRY_MAP = TASK_REGISTRY_MAP.copy()
TEST_TASK_REGISTRY_MAP[Command.LONG_RUNNING_COMMAND] = LongRunningTask


@pytest.fixture(autouse=True, scope="function")
def mock_task_manager(request, persistent, storage, temp_storage):
    """
    Mock celery task manager for testing
    """
    if "disable_task_manager_mock" in request.keywords:
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
                    "temp_storage": temp_storage,
                    "celery": get_celery(),
                    "redis": redis.from_url(TEST_REDIS_URI),
                    "storage": storage,
                    "catalog_id": payload.catalog_id,
                }
                app_container = LazyAppContainer(
                    app_container_config=app_container_config,
                    instance_map=instance_map,
                )
                app_container.override_instance_for_test("task_id", UUID(task_id))
                app_container.override_instance_for_test("progress", AsyncMock())
                task = app_container.get(TEST_TASK_REGISTRY_MAP[payload.command])

                task_result = None
                try:
                    task_payload = task.get_payload_obj(kwargs)
                    task.set_task_id(UUID(task_id))
                    task_result = await task.execute(task_payload)
                    status = TaskStatus.SUCCESS
                    traceback_info = None
                except Exception:
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
                document = task.model_dump(by_alias=True)
                document["_id"] = str(document["_id"])
                await persistent._db[TaskModel.collection_name()].insert_one(document)

                if task_result is not None:
                    updated = await persistent.update_one(
                        collection_name=TaskModel.collection_name(),
                        query_filter={"_id": str(task_id)},
                        update={"$set": {"task_result": task_result}},
                        user_id=user.id,
                    )
                    assert updated == 1, "Task result not updated in persistent storage"
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

                def send_task(*args, **kwargs):
                    task_id = str(uuid4())
                    task_status[task_id] = TaskStatus.STARTED
                    return Mock(id=task_id)

                mock_get_celery.return_value.send_task.side_effect = send_task
                mock_get_celery.return_value.AsyncResult.side_effect = get_task
                mock_get_celery_worker.return_value.AsyncResult.side_effect = get_task
                yield


class MockLogHandler(logging.Handler):
    """
    Mock LogHandler to record logs for testing
    """

    records = []

    def emit(self, record):
        self.records.append(self.format(record))


@pytest.fixture(name="mock_log_handler")
def mock_log_handler_fixture():
    """
    Mock log handler fixture
    """
    mock_handler = MockLogHandler()
    mock_handler.setFormatter(CONSOLE_LOG_FORMATTER)
    mock_handler.records.clear()
    return mock_handler


@pytest.fixture(name="mock_detect_and_update_column_dtypes")
def mock_detect_and_update_column_dtypes_fixture():
    """Mock columns attributes service execution"""
    with patch(
        "featurebyte.service.specialized_dtype.SpecializedDtypeDetectionService.detect_and_update_column_dtypes"
    ):
        yield


@pytest.fixture(name="mysql_online_store_config")
def mysql_online_store_config_fixture():
    """
    MySQL online store config fixture
    """
    return {
        "name": "mysql_online_store",
        "details": MySQLOnlineStoreDetails(
            host="mysql_host",
            database="mysql_database",
            port=3306,
            credential=UsernamePasswordCredential(
                username="mysql_user",
                password="mysql_password",
            ),
        ).model_dump(),
    }


@pytest.fixture(name="mysql_online_store_id")
def mysql_online_store_id_fixture():
    """MySQL online store id"""
    return ObjectId("646f6c190ed28a5271fb02b9")


@pytest.fixture(name="mysql_online_store")
def mysql_online_store_fixture(mysql_online_store_config, mysql_online_store_id):
    """
    Snowflake database source fixture
    """
    try:
        mysql_online_store_config["_id"] = mysql_online_store_id
        online_store = OnlineStore(**mysql_online_store_config)
        online_store.save()
        return online_store
    except (DuplicatedRecordException, ObjectHasBeenSavedError):
        return OnlineStore.get(mysql_online_store_config["name"])


@pytest.fixture(name="mock_update_data_warehouse")
def mock_update_data_warehouse(app_container):
    """Mock update data warehouse method"""

    async def mock_func(feature, target_online_enabled):
        _ = target_online_enabled
        if target_online_enabled:
            precompute_queries = get_online_store_precompute_queries(
                graph=feature.graph,
                node=feature.node,
                source_info=feature.get_source_info(),
                agg_result_name_include_serving_names=feature.agg_result_name_include_serving_names,
            )
            for query in precompute_queries:
                await app_container.online_store_compute_query_service.create_document(query)

    with patch(
        "featurebyte.service.deploy.OnlineEnableService.update_data_warehouse",
        side_effect=mock_func,
    ) as mock_update_data_warehouse:
        yield mock_update_data_warehouse


@pytest.fixture(name="mock_offline_store_feature_manager_dependencies")
def mock_offline_store_feature_manager_dependencies_fixture():
    """
    Fixture to mock dependencies of offline_store_feature_table_manager where database session is
    required and the actual queries will be executed
    """
    patched = {}
    patch_targets = {
        "featurebyte.service.offline_store_feature_table_manager.FeatureMaterializeService": [
            "initialize_new_columns",
            "initialize_precomputed_lookup_feature_table",
            "drop_columns",
            "drop_table",
        ],
        "featurebyte.service.offline_store_feature_table_manager.OfflineStoreFeatureTableCommentService": [
            "apply_comments",
        ],
    }
    started_patchers = []
    for service_name, method_names in patch_targets.items():
        for method_name in method_names:
            patcher = patch(f"{service_name}.{method_name}")
            patched[method_name] = patcher.start()
            started_patchers.append(patcher)
    yield patched
    for patcher in started_patchers:
        patcher.stop()


@pytest.fixture(name="mock_deployment_flow")
def mock_deployment_flow_fixture(
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
    mock_api_object_cache,
):
    """Mock deployment flow"""
    _ = mock_update_data_warehouse, mock_offline_store_feature_manager_dependencies
    _ = mock_api_object_cache
    yield


@pytest.fixture(name="mock_graph_clear_period", autouse=True)
def mock_graph_clear_period_fixture():
    """
    Mock graph clear period
    """
    with patch.dict(os.environ, {"FEATUREBYTE_GRAPH_CLEAR_PERIOD": "1000"}):
        # mock graph clear period to high value to clearing graph in tests
        # clearing graph in tests will cause test failures as the task & client sharing the same process space
        yield


@pytest.fixture(autouse=True)
def patch_app_get_storage(storage, temp_storage):
    """Patch app get storage"""
    with patch("featurebyte.app.get_storage") as mock_get_storage:
        with patch("featurebyte.app.get_temp_storage") as mock_get_temp_storage:
            mock_get_storage.return_value = storage
            mock_get_temp_storage.return_value = temp_storage
            yield


@pytest.fixture(name="mock_is_featurebyte_schema")
def patch_is_featurebyte_schema():
    """Patch is_featurebyte_schema"""
    with patch(
        "featurebyte.service.feature_store_warehouse.FeatureStoreWarehouseService._is_featurebyte_schema"
    ) as mock_is_featurebyte_schema:
        mock_is_featurebyte_schema.return_value = False
        yield mock_is_featurebyte_schema


@pytest.fixture(name="source_info")
def source_info_fixture():
    """Fixture for a default SourceInfo object to use in tests"""
    return SourceInfo(
        database_name="my_db",
        schema_name="my_schema",
        source_type=SourceType.SNOWFLAKE,
    )


@pytest.fixture(name="databricks_source_info")
def databricks_source_info_fixture():
    """Fixture for a Databricks SourceInfo object to use in tests"""
    return SourceInfo(
        database_name="my_db",
        schema_name="my_schema",
        source_type=SourceType.DATABRICKS_UNITY,
    )


@pytest.fixture(name="spark_source_info")
def spark_source_info_fixture():
    """Fixture for a Spark SourceInfo object to use in tests"""
    return SourceInfo(
        database_name="my_db",
        schema_name="my_schema",
        source_type=SourceType.SPARK,
    )


@pytest.fixture(name="bigquery_source_info")
def bigquery_source_info_fixture():
    """Fixture for a BigQuery SourceInfo object to use in tests"""
    return SourceInfo(
        database_name="my_db",
        schema_name="my_schema",
        source_type=SourceType.BIGQUERY,
    )


@pytest.fixture(name="adapter")
def adapter_fixture(source_info):
    """Fixture for a default BaseAdapter object to use in tests"""
    return get_sql_adapter(source_info)


@pytest.fixture(name="session_manager_service")
def session_manager_service_fixture(app_container_no_catalog):
    return app_container_no_catalog.session_manager_service


@pytest.fixture(name="saved_features_set")
def saved_features_set_fixture(float_feature):
    """
    Fixture for a saved feature model
    """
    float_feature.save()
    another_feature = float_feature + 123
    another_feature.name = "another_feature"
    another_feature.save()
    feature_list = FeatureList([float_feature, another_feature], name="my_feature_list")
    feature_list.save()
    feature_cluster = feature_list.cached_model.feature_clusters[0]
    graph = feature_cluster.graph
    node_names = feature_cluster.node_names
    nodes = [graph.get_node_by_name(node_name) for node_name in node_names]
    return graph, nodes, feature_list.feature_names


@pytest.fixture(name="feature_query_generator")
def feature_query_generator_fixture(saved_features_set, source_info):
    """
    Fixture for FeatureQueryGenerator
    """
    graph, nodes, feature_names = saved_features_set
    generator = HistoricalFeatureQueryGenerator(
        graph=graph,
        nodes=nodes,
        request_table_name="my_request_table",
        request_table_columns=["POINT_IN_TIME", "cust_id"],
        source_info=source_info,
        output_table_details=TableDetails(table_name="my_output_table"),
        output_feature_names=feature_names,
    )
    return generator


@pytest.fixture(name="snowflake_time_series_table_development_dataset")
def snowflake_time_series_table_development_dataset_fixture(
    snowflake_feature_store,
    snowflake_time_series_table,
):
    """TimeSeriesTable object fixture"""
    development_dataset = DevelopmentDatasetModel(
        sample_from_timestamp=datetime(2023, 1, 1, 0, 0, 0),
        sample_to_timestamp=datetime(2023, 6, 1, 0, 0, 0),
        development_tables=[
            DevelopmentTable(
                table_id=snowflake_time_series_table.id,
                location=TabularSource(
                    feature_store_id=snowflake_feature_store.id,
                    table_details=TableDetails(
                        database_name="db",
                        schema_name="schema",
                        table_name="sf_time_series_table_dev_sampled",
                    ),
                ),
            )
        ],
    )
    return development_dataset
