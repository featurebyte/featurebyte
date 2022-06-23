"""
Common test fixtures used across unit test directories
"""
from unittest import mock

import pytest

from featurebyte.core.frame import Frame
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph, GlobalQueryGraphState
from featurebyte.tile.snowflake import TileSnowflake


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
