"""
Common test fixtures used across unit test directories
"""
from collections import namedtuple
from unittest import mock
import json
import tempfile

import pandas as pd
import pytest
import yaml

from featurebyte.api.event_view import EventView
from featurebyte.config import Configurations
from featurebyte.core.frame import Frame
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph, GlobalQueryGraphState, Node
from featurebyte.tile.snowflake import TileSnowflake


@pytest.fixture(name="config")
def config_fixture():
    """
    Create config object for testing
    """
    config_dict = {
        "datasource": [
            {
                "name": "sf_datasource",
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
                "name": "sq_datasource",
                "source_type": "sqlite",
                "filename": "some_filename.sqlite",
            },
        ],
    }
    with tempfile.NamedTemporaryFile("w") as file_handle:
        file_handle.write(yaml.dump(config_dict))
        file_handle.flush()
        yield Configurations(config_file_path=file_handle.name)


@pytest.fixture(name="snowflake_datasource")
def snowflake_datasource_fixture(config):
    """
    Snowflake database source fixture
    """
    return config.db_sources["sf_datasource"]


@pytest.fixture(name="sqlite_datasource")
def sqlite_datasource_fixture(config):
    """
    Snowflake database source fixture
    """
    return config.db_sources["sq_datasource"]


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
        }
        return pd.DataFrame(query_map[query])

    with mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query") as mock_execute_query:
        mock_execute_query.side_effect = side_effect
        yield mock_execute_query


@pytest.fixture(name="graph")
def query_graph():
    """
    Empty query graph fixture
    """
    GlobalQueryGraphState.reset()
    yield GlobalQueryGraph()


@pytest.fixture(name="dataframe")
def dataframe_fixture(graph):
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
            "dbtable": "transaction",
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    yield Frame(
        node=node,
        column_var_type_map=column_var_type_map,
        column_lineage_map={col: (node.name,) for col in column_var_type_map},
        row_index_lineage=(node.name,),
    )


@pytest.fixture(name="session")
def fake_session():
    """
    Fake database session for testing
    """
    FakeSession = namedtuple("FakeSession", ["database_metadata", "source_type"])
    database_metadata = {
        '"trans"': {
            "cust_id": DBVarType.INT,
            "session_id": DBVarType.INT,
            "event_type": DBVarType.VARCHAR,
            "value": DBVarType.FLOAT,
            "created_at": DBVarType.INT,
        }
    }
    yield FakeSession(database_metadata=database_metadata, source_type="sqlite")


@pytest.fixture(name="event_view")
def event_view_fixture(session, graph):
    """
    EventView fixture
    """
    event_view = EventView.from_session(
        session=session,
        table_name='"trans"',
        timestamp_column="created_at",
        entity_identifiers=["cust_id"],
    )
    assert isinstance(event_view, EventView)
    expected_inception_node = Node(
        name="input_1",
        type=NodeType.INPUT,
        parameters={
            "columns": ["cust_id", "session_id", "event_type", "value", "created_at"],
            "timestamp": "created_at",
            "entity_identifiers": ["cust_id"],
            "dbtable": '"trans"',
        },
        output_type=NodeOutputType.FRAME,
    )
    assert event_view.graph.dict() == graph.dict()
    assert event_view.protected_columns == {"created_at", "cust_id"}
    assert event_view.inception_node == expected_inception_node
    assert event_view.timestamp_column == "created_at"
    assert event_view.entity_identifiers == ["cust_id"]
    yield event_view


@pytest.fixture
@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
@mock.patch("featurebyte.session.snowflake.SnowflakeSession")
def mock_snowflake_tile(mock_execute_query, mock_snowflake_session):
    """
    Pytest Fixture for TileSnowflake instance
    """
    mock_snowflake_session.warehouse = "warehouse"
    mock_execute_query.size_effect = None

    tile_s = TileSnowflake(
        mock_snowflake_session,
        "featurename",
        183,
        3,
        5,
        "select c1 from dummy where tile_start_ts >= FB_START_TS and tile_start_ts < FB_END_TS",
        "c1",
        "tile_id1",
    )
    return tile_s
