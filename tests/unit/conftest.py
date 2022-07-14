"""
Common test fixtures used across unit test directories
"""
import json
import tempfile
from unittest import mock

import pandas as pd
import pytest
import yaml

from featurebyte.api.entity import Entity
from featurebyte.api.event_data import EventData
from featurebyte.api.event_view import EventView
from featurebyte.api.feature import Feature, FeatureGroup
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.groupby import EventViewGroupBy
from featurebyte.config import Configurations
from featurebyte.core.frame import Frame
from featurebyte.enum import CollectionName, DBVarType, InternalName
from featurebyte.feature_manager.snowflake_feature import FeatureManagerSnowflake
from featurebyte.feature_manager.snowflake_feature_list import FeatureListManagerSnowflake
from featurebyte.models.feature import FeatureListModel, FeatureListStatus, FeatureReadiness
from featurebyte.models.tile import TileSpec
from featurebyte.persistent.git import GitDB
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph, GlobalQueryGraphState, Node
from featurebyte.session.manager import SessionManager
from featurebyte.tile.snowflake_tile import TileManagerSnowflake


@pytest.fixture(name="config_file")
def config_file_fixture():
    """
    Config file for unit testing
    """
    config_dict = {
        "featurestore": [
            {
                "name": "sf_featurestore",
                "source_type": "snowflake",
                "account": "sf_account",
                "warehouse": "sf_warehouse",
                "sf_schema": "sf_schema",
                "database": "sf_database",
                "credential_type": "USERNAME_PASSWORD",
                "username": "sf_user",
                "password": "sf_password",
            },
            {
                "name": "sq_featurestore",
                "source_type": "sqlite",
                "filename": "some_filename.sqlite",
            },
        ],
    }
    with tempfile.NamedTemporaryFile("w") as file_handle:
        file_handle.write(yaml.dump(config_dict))
        file_handle.flush()
        yield file_handle.name


@pytest.fixture(name="config")
def config_fixture(config_file):
    """
    Config object for unit testing
    """
    yield Configurations(config_file_path=config_file)


@pytest.fixture(name="mock_config_path_env")
def mock_config_path_env_fixture(config_file):
    """
    Mock FEATUREBYTE_CONFIG_PATH in featurebyte/config.py
    """
    with mock.patch("featurebyte.config.os.environ.get") as mock_env_get:
        mock_env_get.return_value = config_file
        yield


@pytest.fixture(name="graph")
def query_graph():
    """
    Empty query graph fixture
    """
    GlobalQueryGraphState.reset()
    yield GlobalQueryGraph()


@pytest.fixture(name="snowflake_feature_store")
def snowflake_feature_store_fixture(config, graph):
    """
    Snowflake database source fixture
    """
    _ = graph
    return FeatureStore(**config.feature_stores["sf_featurestore"].dict())


@pytest.fixture(name="snowflake_connector")
def mock_snowflake_connector():
    """
    Mock snowflake connector in featurebyte.session.snowflake module
    """
    with mock.patch("featurebyte.session.snowflake.connector") as mock_connector:
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


@pytest.fixture(name="snowflake_database_table")
def snowflake_database_table_fixture(
    snowflake_connector, snowflake_execute_query, snowflake_feature_store, config
):
    """
    DatabaseTable object fixture
    """
    _ = snowflake_connector, snowflake_execute_query
    yield snowflake_feature_store.get_table(
        database_name="sf_database",
        schema_name="sf_schema",
        table_name="sf_table",
        credentials=config.credentials,
    )


@pytest.fixture(name="snowflake_event_data")
def snowflake_event_data_fixture(snowflake_database_table, config, mock_get_persistent):
    """
    EventData object fixture
    """
    _ = mock_get_persistent
    event_data = EventData.from_tabular_source(
        tabular_source=snowflake_database_table,
        name="sf_event_data",
        event_timestamp_column="event_timestamp",
        record_creation_date_column="created_at",
        credentials=config.credentials,
    )
    return event_data


@pytest.fixture(name="snowflake_event_view")
def snowflake_event_view_fixture(snowflake_event_data, config):
    """
    EventData object fixture
    """
    _ = config
    event_view = EventView.from_event_data(event_data=snowflake_event_data)
    assert isinstance(event_view, EventView)
    expected_inception_node = Node(
        name="input_2",
        type=NodeType.INPUT,
        parameters={
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
            "database_source": {
                "type": "snowflake",
                "details": {
                    "account": "sf_account",
                    "database": "sf_database",
                    "sf_schema": "sf_schema",
                    "warehouse": "sf_warehouse",
                },
            },
            "dbtable": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "sf_table",
            },
        },
        output_type=NodeOutputType.FRAME,
    )
    assert event_view.inception_node == expected_inception_node
    assert event_view.protected_columns == {"event_timestamp"}
    assert event_view.inherited_columns == {"event_timestamp"}
    assert event_view.timestamp_column == "event_timestamp"
    yield event_view


@pytest.fixture(name="grouped_event_view")
def grouped_event_view_fixture(snowflake_event_view, mock_get_persistent):
    """
    EventViewGroupBy fixture
    """
    _ = mock_get_persistent
    Entity.create(name="customer", serving_name="cust_id")
    snowflake_event_view.cust_id.as_entity("customer")
    grouped = snowflake_event_view.groupby("cust_id")
    assert isinstance(grouped, EventViewGroupBy)
    yield grouped


@pytest.fixture(name="feature_group")
def feature_group_fixture(grouped_event_view):
    """
    FeatureList fixture
    """
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
    expected_inception_node = Node(
        name="groupby_2",
        type=NodeType.GROUPBY,
        parameters={
            "keys": ["cust_id"],
            "parent": "col_float",
            "agg_func": "sum",
            "value_by": None,
            "windows": ["30m", "2h", "1d"],
            "timestamp": "event_timestamp",
            "blind_spot": 600,
            "time_modulo_frequency": 300,
            "frequency": 1800,
            "names": ["sum_30m", "sum_2h", "sum_1d"],
            "tile_id": "sum_f1800_m300_b600_3cb3b2b28a359956be02abe635c4446cb50710d7",
        },
        output_type=NodeOutputType.FRAME,
    )
    assert isinstance(feature_group, FeatureGroup)
    assert feature_group.protected_columns == {"cust_id"}
    assert feature_group.inherited_columns == {"cust_id"}
    assert feature_group.inception_node == expected_inception_node
    assert feature_group.entity_identifiers == ["cust_id"]
    assert feature_group.columns == ["cust_id", "sum_30m", "sum_2h", "sum_1d"]
    assert feature_group.column_lineage_map == {
        "cust_id": ("groupby_2",),
        "sum_30m": ("groupby_2",),
        "sum_2h": ("groupby_2",),
        "sum_1d": ("groupby_2",),
    }
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
    assert feature.inception_node == feature_group.inception_node
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
    assert bool_feature.inception_node == float_feature.inception_node
    yield bool_feature


@pytest.fixture(name="dataframe")
def dataframe_fixture(graph, snowflake_feature_store):
    """
    Frame test fixture
    """
    column_var_type_map = {
        "CUST_ID": DBVarType.INT,
        "PRODUCT_ACTION": DBVarType.VARCHAR,
        "VALUE": DBVarType.FLOAT,
        "MASK": DBVarType.BOOL,
    }
    node = graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "columns": list(column_var_type_map.keys()),
            "timestamp": "VALUE",
            "dbtable": {
                "database_name": "db",
                "schema_name": "public",
                "table_name": "transaction",
            },
            "database_source": {
                "type": "snowflake",
                "details": {
                    "database": "db",
                    "sf_schema": "public",
                },
            },
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    yield Frame(
        tabular_source=(
            snowflake_feature_store,
            {"database_name": "db", "schema_name": "public", "table_name": "some_table_name"},
        ),
        node=node,
        column_var_type_map=column_var_type_map,
        column_lineage_map={col: (node.name,) for col in column_var_type_map},
        row_index_lineage=(node.name,),
    )


@pytest.fixture(name="session_manager")
def session_manager_fixture(config, snowflake_connector):
    """
    Session manager fixture
    """
    # pylint: disable=E1101
    _ = snowflake_connector
    SessionManager.__getitem__.cache_clear()
    yield SessionManager(credentials=config.credentials)


@pytest.fixture(name="mock_get_persistent")
def mock_get_persistent_fixture():
    """
    Mock _get_persistent for testing
    """
    with mock.patch("featurebyte.app._get_persistent") as mock_get_persistent:
        gitdb = GitDB()
        gitdb.insert_doc_name_func(CollectionName.EVENT_DATA, lambda doc: doc["name"])
        mock_get_persistent.return_value = gitdb
        yield mock_get_persistent


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
        value_column_names=["col2"],
        entity_column_names=["col1"],
    )

    return tile_spec


@pytest.fixture
@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
def tile_manager(mock_execute_query, session_manager, snowflake_feature_store):
    """
    Tile Manager fixture
    """
    _ = mock_execute_query
    return TileManagerSnowflake(session=session_manager[snowflake_feature_store])


@pytest.fixture
@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
def mock_snowflake_feature(
    mock_execute_query, snowflake_connector, snowflake_event_view, mock_get_persistent
):
    """Fixture for a Feature object"""
    mock_execute_query.size_effect = None
    _ = snowflake_connector, mock_get_persistent

    Entity.create(name="customer", serving_name="cust_id")
    snowflake_event_view.cust_id.as_entity("customer")
    feature_group = snowflake_event_view.groupby(by_keys="cust_id").aggregate(
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
    feature.online_enabled = False
    return feature


@pytest.fixture
@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
def feature_manager(mock_execute_query, session_manager, snowflake_feature_store):
    """
    Feature Manager fixture
    """
    _ = mock_execute_query
    return FeatureManagerSnowflake(session=session_manager[snowflake_feature_store])


@pytest.fixture
@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
def mock_snowflake_feature_list_model(
    mock_execute_query, snowflake_connector, snowflake_event_view, mock_get_persistent
):
    """Fixture for a FeatureListModel"""
    mock_execute_query.size_effect = None
    _ = snowflake_connector, mock_get_persistent

    Entity.create(name="customer", serving_name="cust_id")
    snowflake_event_view.cust_id.as_entity("customer")
    feature_group = snowflake_event_view.groupby(by_keys="cust_id").aggregate(
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

    mock_feature_list = FeatureListModel(
        name="feature_list1",
        description="test_description1",
        features=[(feature.name, feature.version)],
        readiness=FeatureReadiness.DRAFT,
        status=FeatureListStatus.DRAFT,
        version="v1",
    )

    return mock_feature_list


@pytest.fixture
@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
def feature_list_manager(mock_execute_query, session_manager, snowflake_feature_store):
    """
    Feature List Manager fixture
    """
    _ = mock_execute_query
    return FeatureListManagerSnowflake(session=session_manager[snowflake_feature_store])


@pytest.fixture(name="git_persistent")
def git_persistent_fixture():
    """
    Patched MongoDB fixture for testing
    Returns
    -------
    Tuple[GitDB, Repo]
        Local GitDB object and local git repo
    """
    persistent = GitDB(branch="test")
    persistent.insert_doc_name_func("event_data", lambda doc: doc["name"])
    persistent.insert_doc_name_func("data", lambda doc: doc["name"])
    yield persistent, persistent.repo


@pytest.fixture(name="mocked_tile_cache")
def mocked_tile_cache_fixture():
    """Fixture for a mocked SnowflakeTileCache object"""
    with mock.patch(
        "featurebyte.query_graph.feature_historical.SnowflakeTileCache", autospec=True
    ) as mocked_cls:
        yield mocked_cls.return_value
