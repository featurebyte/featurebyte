import pytest

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from tests.util.helper import reset_global_graph


@pytest.fixture(name="graph", scope="function")
def query_graph():
    """
    Empty query graph fixture
    """
    yield reset_global_graph()


@pytest.fixture(name="node_input")
def node_input_fixture(graph):
    """Fixture for a generic input node"""
    node_params = {
        "type": "event_table",
        "columns": [
            {"name": "ts", "dtype": "TIMESTAMP"},
            {"name": "cust_id", "dtype": "VARCHAR"},
            {"name": "a", "dtype": "FLOAT"},
            {"name": "b", "dtype": "INT"},
        ],
        "timestamp_column": "ts",
        "table_details": {
            "database_name": "db",
            "schema_name": "public",
            "table_name": "event_table",
        },
        "feature_store_details": {
            "type": "snowflake",
            "details": {
                "database": "db",
                "sf_schema": "public",
                "account": "account",
                "warehouse": "warehouse",
            },
        },
    }
    node_input = graph.add_operation(
        node_type=NodeType.INPUT,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    return node_input


@pytest.fixture(name="simple_graph", scope="function")
def simple_graph_fixture(graph, node_input):
    """Simple graph"""
    proj_a = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    assign = graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "a_copy"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input, proj_a],
    )
    return graph, assign
