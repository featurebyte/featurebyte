"""
Common test fixtures used across unit test directories
"""
from collections import namedtuple

import pytest

from featurebyte.api.event_view import EventView
from featurebyte.core.frame import Frame
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph, GlobalQueryGraphState, Node


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
