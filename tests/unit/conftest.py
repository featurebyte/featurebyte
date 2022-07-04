"""
Common test fixtures used across unit test directories
"""
import json
import tempfile
from unittest import mock

import pandas as pd
import pytest
import yaml

from featurebyte.api.event_data import EventData
from featurebyte.api.event_view import EventView
from featurebyte.api.feature_store import FeatureStore
from featurebyte.config import Configurations
from featurebyte.core.frame import Frame
from featurebyte.enum import DBVarType
from featurebyte.feature_manager.snowflake_feature import FeatureSnowflake
from featurebyte.models.event_data import Feature, TileSpec
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph, GlobalQueryGraphState, Node
from featurebyte.session.manager import SessionManager
from featurebyte.tile.snowflake_tile import TileSnowflake


@pytest.fixture(name="config")
def config_fixture():
    """
    Config object for unit testing
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
        yield Configurations(config_file_path=file_handle.name)


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
def snowflake_event_data_fixture(snowflake_database_table, config):
    """
    EventData object fixture
    """
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
    assert event_view.timestamp_column == "event_timestamp"
    yield event_view


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


@pytest.fixture
@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
def mock_snowflake_tile(mock_execute_query, snowflake_feature_store, snowflake_connector, config):
    """
    Pytest Fixture for TileSnowflake instance
    """
    mock_execute_query.size_effect = None
    _ = snowflake_connector

    tile_s = TileSnowflake(
        time_modulo_frequency_seconds=183,
        blind_spot_seconds=3,
        frequency_minute=5,
        tile_sql="select c1 from dummy where tile_start_ts >= FB_START_TS and tile_start_ts < FB_END_TS",
        column_names="c1",
        tile_id="tile_id1",
        tabular_source=snowflake_feature_store,
        credentials=config.credentials,
    )

    return tile_s


@pytest.fixture
@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
def mock_snowflake_feature(
    mock_execute_query, snowflake_database_source, snowflake_connector, config
):
    """
    Pytest Fixture for FeatureSnowflake instance
    """
    mock_execute_query.size_effect = None
    _ = snowflake_connector

    tile_id = "tile_id1"

    tile_spec = TileSpec(
        tile_id=tile_id,
        time_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        tile_sql="SELECT * FROM DUMMY",
        column_names="col1",
    )

    feature = Feature(
        name="test_feature1",
        version="v1",
        readiness="DRAFT",
        is_default=True,
        tile_specs=[tile_spec],
        online_enabled=False,
        datasource=snowflake_feature_store,
    )

    s_feature = FeatureSnowflake(feature=feature, credentials=config.credentials)

    return s_feature
