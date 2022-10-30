"""
Common test fixtures used across unit test directories
"""
import json
import os
import tempfile
from unittest import mock

import pandas as pd
import pytest
import pytest_asyncio
import yaml
from bson.objectid import ObjectId
from fastapi.testclient import TestClient
from snowflake.connector.constants import QueryStatus

from featurebyte.api.entity import Entity
from featurebyte.api.event_data import EventData
from featurebyte.api.event_view import EventView
from featurebyte.api.feature import DefaultVersionMode, Feature
from featurebyte.api.feature_list import FeatureGroup, FeatureList
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.groupby import EventViewGroupBy
from featurebyte.app import app
from featurebyte.common.model_util import get_version
from featurebyte.config import Configurations
from featurebyte.enum import DBVarType, InternalName
from featurebyte.feature_manager.model import ExtendedFeatureListModel
from featurebyte.feature_manager.snowflake_feature import FeatureManagerSnowflake
from featurebyte.feature_manager.snowflake_feature_list import FeatureListManagerSnowflake
from featurebyte.models.base import VersionIdentifier
from featurebyte.models.feature import FeatureReadiness
from featurebyte.models.feature_list import FeatureListNamespaceModel, FeatureListStatus
from featurebyte.models.feature_store import SnowflakeDetails
from featurebyte.models.tile import TileSpec
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.node import construct_node
from featurebyte.schema.feature_job_setting_analysis import FeatureJobSettingAnalysisCreate
from featurebyte.schema.feature_namespace import FeatureNamespaceCreate
from featurebyte.session.manager import SessionManager, session_cache
from featurebyte.storage.local import LocalStorage
from featurebyte.tile.snowflake_tile import TileManagerSnowflake

# register tests.unit.routes.base so that API stacktrace display properly
pytest.register_assert_rewrite("tests.unit.routes.base")


@pytest.fixture(name="config_file")
def config_file_fixture():
    """
    Config file for unit testing
    """
    config_dict = {
        "featurestore": [
            {
                "name": "sf_featurestore",
                "credential_type": "USERNAME_PASSWORD",
                "username": "sf_user",
                "password": "sf_password",
            },
            {
                "name": "sq_featurestore",
            },
        ],
        "profile": [
            {
                "name": "local",
                "api_url": "http://localhost:8080",
                "api_token": "token",
            },
        ],
    }
    with tempfile.TemporaryDirectory() as tempdir:
        config_file_path = os.path.join(tempdir, "config.yaml")
        with open(config_file_path, "w") as file_handle:
            file_handle.write(yaml.dump(config_dict))
            file_handle.flush()
            yield config_file_path


@pytest.fixture(name="config")
def config_fixture(config_file):
    """
    Config object for unit testing
    """
    yield Configurations(config_file_path=config_file)


@pytest.fixture(name="mock_config_path_env")
def mock_config_path_env_fixture(config_file):
    """
    Mock FEATUREBYTE_HOME in featurebyte/config.py
    """

    def mock_env_side_effect(*args, **kwargs):
        if args[0] == "FEATUREBYTE_HOME":
            return os.path.dirname(config_file)
        env = dict(os.environ)
        return env.get(*args, **kwargs)

    with mock.patch("featurebyte.config.os.environ.get") as mock_env_get:
        mock_env_get.side_effect = mock_env_side_effect
        yield


@pytest.fixture(autouse=True)
def mock_api_client_fixture():
    """
    Mock Configurations.get_client to use test client
    """
    with mock.patch("featurebyte.config.APIClient.request") as mock_request:
        with TestClient(app) as client:
            mock_request.side_effect = client.request
            yield


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


@pytest.fixture(name="snowflake_execute_query")
def mock_snowflake_execute_query():
    """
    Mock execute_query in featurebyte.session.snowflake.SnowflakeSession class
    """

    def side_effect(query):
        query_map = {
            "SHOW DATABASES": [{"name": "sf_database"}],
            'SHOW SCHEMAS IN DATABASE "sf_database"': [{"name": "sf_schema"}],
            'SHOW TABLES IN SCHEMA "sf_database"."sf_schema"': [{"name": "sf_table"}],
            'SHOW VIEWS IN SCHEMA "sf_database"."sf_schema"': [{"name": "sf_view"}],
            'SHOW COLUMNS IN "sf_database"."sf_schema"."sf_table"': [
                {"column_name": "col_int", "data_type": json.dumps({"type": "FIXED"})},
                {"column_name": "col_float", "data_type": json.dumps({"type": "REAL"})},
                {"column_name": "col_char", "data_type": json.dumps({"type": "TEXT", "length": 1})},
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
                {"column_name": "cust_id", "data_type": json.dumps({"type": "FIXED"})},
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
            "SHOW SCHEMAS": [
                {"name": "PUBLIC"},
            ],
        }
        res = query_map.get(query)
        if res is not None:
            return pd.DataFrame(res)
        return None

    with mock.patch(
        "featurebyte.session.snowflake.SnowflakeSession.execute_query"
    ) as mock_execute_query:
        mock_execute_query.side_effect = side_effect
        yield mock_execute_query


@pytest.fixture(name="snowflake_feature_store")
def snowflake_feature_store_fixture():
    """
    Snowflake database source fixture
    """
    return FeatureStore(
        name="sf_featurestore",
        type="snowflake",
        details=SnowflakeDetails(
            account="sf_account",
            warehouse="sf_warehouse",
            sf_schema="sf_schema",
            database="sf_database",
        ),
    )


@pytest.fixture(name="snowflake_database_table")
def snowflake_database_table_fixture(
    snowflake_connector,
    snowflake_execute_query,
    snowflake_feature_store,
):
    """
    DatabaseTable object fixture
    """
    _ = snowflake_connector, snowflake_execute_query
    snowflake_table = snowflake_feature_store.get_table(
        database_name="sf_database",
        schema_name="sf_schema",
        table_name="sf_table",
    )
    assert isinstance(snowflake_table.feature_store, FeatureStore)
    yield snowflake_table


@pytest.fixture(name="snowflake_event_data_id")
def snowflake_event_data_id_fixture():
    """Snowflake event data ID"""
    return ObjectId("6337f9651050ee7d5980660d")


@pytest.fixture(name="snowflake_event_data")
def snowflake_event_data_fixture(snowflake_database_table, snowflake_event_data_id):
    """EventData object fixture"""
    event_data = EventData.from_tabular_source(
        tabular_source=snowflake_database_table,
        name="sf_event_data",
        event_id_column="col_int",
        event_timestamp_column="event_timestamp",
        record_creation_date_column="created_at",
        _id=snowflake_event_data_id,
    )
    assert event_data.node.parameters.id == event_data.id
    yield event_data


@pytest.fixture(name="cust_id_entity")
def cust_id_entity_fixture():
    """
    Customer ID entity fixture
    """
    entity = Entity(name="customer", serving_names=["cust_id"])
    entity.save()
    yield entity


@pytest.fixture(name="snowflake_event_data_with_entity")
def snowflake_event_data_with_entity_fixture(snowflake_event_data, cust_id_entity):
    """
    Entity fixture that sets cust_id in snowflake_event_data as an Entity
    """
    snowflake_event_data.cust_id.as_entity(cust_id_entity.name)
    yield snowflake_event_data


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


@pytest.fixture(name="snowflake_event_view")
def snowflake_event_view_fixture(
    snowflake_event_data, snowflake_feature_store_details_dict, snowflake_table_details_dict
):
    """
    EventData object fixture
    """
    event_view = EventView.from_event_data(event_data=snowflake_event_data)
    assert isinstance(event_view, EventView)
    expected_input_node = construct_node(
        name="input_2",
        type=NodeType.INPUT,
        parameters={
            "type": "event_data",
            "id": snowflake_event_data.id,
            "columns": [
                "col_int",
                "col_float",
                "col_char",
                "col_text",
                "col_binary",
                "col_boolean",
                "event_timestamp",
                "created_at",
                "cust_id",
            ],
            "timestamp": "event_timestamp",
            "feature_store_details": snowflake_feature_store_details_dict,
            "table_details": snowflake_table_details_dict,
        },
        output_type=NodeOutputType.FRAME,
    ).dict(exclude={"name": True})
    for input_node in event_view.graph.iterate_nodes(
        target_node=event_view.node, node_type=NodeType.INPUT
    ):
        assert input_node.dict(exclude={"name": True}) == expected_input_node
    assert event_view.protected_columns == {"event_timestamp"}
    assert event_view.inherited_columns == {"event_timestamp"}
    assert event_view.timestamp_column == "event_timestamp"
    assert event_view.event_data_id == snowflake_event_data.id
    yield event_view


@pytest.fixture(name="snowflake_event_view_with_entity")
def snowflake_event_view_entity_fixture(snowflake_event_data_with_entity):
    """
    Snowflake event view with entity
    """
    event_view = EventView.from_event_data(event_data=snowflake_event_data_with_entity)
    assert event_view.node.parameters.id == snowflake_event_data_with_entity.id
    yield event_view


@pytest.fixture(name="grouped_event_view")
def grouped_event_view_fixture(snowflake_event_view_with_entity):
    """
    EventViewGroupBy fixture
    """
    grouped = snowflake_event_view_with_entity.groupby("cust_id")
    assert isinstance(grouped, EventViewGroupBy)
    assert snowflake_event_view_with_entity.event_data_id == grouped.obj.event_data_id
    yield grouped


@pytest.fixture(name="feature_group")
def feature_group_fixture(grouped_event_view, cust_id_entity, snowflake_event_data_with_entity):
    """
    FeatureList fixture
    """
    global_graph = GlobalQueryGraph()
    assert id(global_graph.nodes) == id(grouped_event_view.obj.graph.nodes)
    feature_group = grouped_event_view.aggregate(
        value_column="col_float",
        method="sum",
        windows=["30m", "2h", "1d"],
        feature_job_setting={
            "blind_spot": "10m",
            "frequency": "30m",
            "time_modulo_frequency": "5m",
        },
        feature_names=["sum_30m", "sum_2h", "sum_1d"],
    )
    assert isinstance(feature_group, FeatureGroup)
    for feature in feature_group.feature_objects.values():
        assert grouped_event_view.obj.event_data_id in feature.event_data_ids
        assert id(feature.graph.nodes) == id(global_graph.nodes)
        assert feature.event_data_ids == [snowflake_event_data_with_entity.id]
        assert feature.entity_ids == [cust_id_entity.id]
    yield feature_group


@pytest.fixture(name="float_feature")
def float_feature_fixture(feature_group):
    """
    Float Feature fixture
    """
    feature = feature_group["sum_1d"]
    assert isinstance(feature, Feature)
    assert feature.protected_columns == {"cust_id"}
    assert feature.inherited_columns == {"cust_id"}
    assert feature_group["sum_1d"].event_data_ids == feature.event_data_ids
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
    assert bool_feature.event_data_ids == float_feature.event_data_ids
    yield bool_feature


@pytest.fixture(name="agg_per_category_feature")
def agg_per_category_feature_fixture(snowflake_event_view_with_entity):
    """
    Aggregation per category feature fixture
    """
    grouped = snowflake_event_view_with_entity.groupby("cust_id", category="col_int")
    features = grouped.aggregate(
        value_column="col_float",
        method="sum",
        windows=["30m", "2h", "1d"],
        feature_job_setting={
            "blind_spot": "10m",
            "frequency": "30m",
            "time_modulo_frequency": "5m",
        },
        feature_names=["sum_30m", "sum_2h", "sum_1d"],
    )
    yield features["sum_1d"]


@pytest.fixture(name="count_per_category_feature_group")
def count_per_category_feature_group_fixture(snowflake_event_view_with_entity):
    """
    Aggregation per category FeatureGroup fixture
    """
    grouped = snowflake_event_view_with_entity.groupby("cust_id", category="col_int")
    features = grouped.aggregate(
        method="count",
        windows=["30m", "2h", "1d"],
        feature_job_setting={
            "blind_spot": "10m",
            "frequency": "30m",
            "time_modulo_frequency": "5m",
        },
        feature_names=["counts_30m", "counts_2h", "counts_1d"],
    )
    yield features


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
def session_manager_fixture(config, snowflake_connector):
    """
    Session manager fixture
    """
    # pylint: disable=E1101
    _ = snowflake_connector
    session_cache.clear()
    yield SessionManager(credentials=config.credentials)


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
        tile_id="tile_id1",
        aggregation_id="agg_id1",
        value_column_names=["col2"],
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
    return TileManagerSnowflake(session=await session_manager.get_session(snowflake_feature_store))


@pytest.fixture
@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
def mock_snowflake_feature(
    mock_execute_query, snowflake_connector, snowflake_event_view_with_entity
):
    """Fixture for a Feature object"""
    mock_execute_query.size_effect = None
    _ = snowflake_connector

    feature_group = snowflake_event_view_with_entity.groupby(by_keys="cust_id").aggregate(
        value_column="col_float",
        method="sum",
        windows=["30m"],
        feature_names=["sum_30m"],
        feature_job_setting={
            "blind_spot": "10m",
            "frequency": "30m",
            "time_modulo_frequency": "5m",
        },
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
    return FeatureManagerSnowflake(
        session=await session_manager.get_session(snowflake_feature_store)
    )


@pytest.fixture
@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
def mock_snowflake_feature_list_model(
    mock_execute_query, snowflake_connector, snowflake_event_view_with_entity
):
    """Fixture for a FeatureListModel"""
    mock_execute_query.size_effect = None
    _ = snowflake_connector

    feature_group = snowflake_event_view_with_entity.groupby(by_keys="cust_id").aggregate(
        value_column="col_float",
        method="sum",
        windows=["30m"],
        feature_names=["sum_30m"],
        feature_job_setting={
            "blind_spot": "10m",
            "frequency": "30m",
            "time_modulo_frequency": "5m",
        },
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


@pytest_asyncio.fixture
@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
async def feature_list_manager(mock_execute_query, session_manager, snowflake_feature_store):
    """
    Feature List Manager fixture
    """
    _ = mock_execute_query
    return FeatureListManagerSnowflake(
        session=await session_manager.get_session(snowflake_feature_store)
    )


@pytest.fixture(name="mocked_tile_cache")
def mocked_tile_cache_fixture():
    """Fixture for a mocked SnowflakeTileCache object"""
    with mock.patch(
        "featurebyte.query_graph.sql.feature_historical.get_tile_cache"
    ) as mocked_get_tile_cache:

        async def _mock_compute_tiles_on_demand(*args, **kwargs):
            _ = args
            _ = kwargs
            return None

        mocked_tile_cache = mock.Mock()
        mocked_tile_cache.compute_tiles_on_demand.side_effect = _mock_compute_tiles_on_demand
        mocked_get_tile_cache.return_value = mocked_tile_cache
        yield mocked_tile_cache


def test_save_payload_fixtures(
    update_fixtures,
    snowflake_feature_store,
    snowflake_event_data,
    feature_group,
    cust_id_entity,
):
    """
    Write request payload for testing api route
    """
    # pylint: disable=too-many-locals
    feature_sum_30m = feature_group["sum_30m"]
    feature_sum_2h = feature_group["sum_2h"]
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
        event_data_ids=feature_sum_30m.event_data_ids,
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
        event_data_ids=feature_sum_30m.event_data_ids,
        feature_namespace_ids=[
            feature_sum_30m.feature_namespace_id,
            feature_sum_2h.feature_namespace_id,
        ],
    )
    feature_job_setting_analysis = FeatureJobSettingAnalysisCreate(
        _id="62f301e841b73757c9ff879a",
        user_id="62f302f841b73757c9ff876b",
        name="sample_analysis",
        event_data_id=snowflake_event_data.id,
        analysis_data=None,
        analysis_length=2419200,
        min_featurejob_period=60,
        exclude_late_job=False,
        blind_spot_buffer_setting=5,
        job_time_buffer_setting="auto",
        late_data_allowance=5e-05,
    )

    if update_fixtures:
        api_object_name_pairs = [
            (cust_id_entity, "entity"),
            (snowflake_feature_store, "feature_store"),
            (snowflake_event_data, "event_data"),
            (feature_sum_30m, "feature_sum_30m"),
            (feature_sum_2h, "feature_sum_2h"),
            (feature_list, "feature_list_single"),
            (feature_list_multiple, "feature_list_multi"),
        ]
        output_filenames = []
        base_path = "tests/fixtures/request_payloads"
        for api_object, name in api_object_name_pairs:
            filename = f"{base_path}/{name}.json"
            with open(filename, "w") as fhandle:
                fhandle.write(
                    json.dumps(api_object._get_create_payload(), indent=4, sort_keys=True)
                )
            output_filenames.append(filename)

        json_filename = f"{base_path}/feature_namespace.json"
        with open(json_filename, "w") as fhandle:
            fhandle.write(json.dumps(feature_namespace.json_dict(), indent=4, sort_keys=True))
            output_filenames.append(json_filename)

        json_filename = f"{base_path}/feature_list_namespace.json"
        with open(json_filename, "w") as fhandle:
            fhandle.write(json.dumps(feature_list_namespace.json_dict(), indent=4, sort_keys=True))
            output_filenames.append(json_filename)

        json_filename = f"{base_path}/feature_job_setting_analysis.json"
        with open(json_filename, "w") as fhandle:
            fhandle.write(
                json.dumps(feature_job_setting_analysis.json_dict(), indent=4, sort_keys=True)
            )
            output_filenames.append(json_filename)

        raise AssertionError(
            f"Fixtures {output_filenames} updated, please set update_fixture to False"
        )
