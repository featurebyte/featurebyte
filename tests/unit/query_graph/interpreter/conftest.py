import pytest

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from tests.util.helper import reset_global_graph


@pytest.fixture(name="graph", scope="function")
def query_graph():
    """
    Empty query graph fixture
    """
    yield reset_global_graph()


@pytest.fixture(name="has_timestamp_schema")
def has_timestamp_schema_fixture(graph):
    """
    Fixture that determines if a timestamp node has a timestamp schema. Can be overridden by tests.
    """
    return False


@pytest.fixture(name="node_input")
def node_input_fixture(graph, has_timestamp_schema):
    """Fixture for a generic input node"""
    ts_column_info = {"name": "ts", "dtype": "TIMESTAMP"}
    if has_timestamp_schema:
        ts_column_info["dtype"] = "VARCHAR"
        ts_column_info["dtype_metadata"] = {
            "timestamp_schema": {
                "format_string": "%Y-%m-%d %H:%M:%S",
                "timezone": "UTC",
            }
        }
    node_params = {
        "type": "event_table",
        "columns": [
            ts_column_info,
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
                "database_name": "db",
                "schema_name": "public",
                "role_name": "role_name",
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


@pytest.fixture(name="project_from_simple_graph", scope="function")
def project_from_simple_graph_fixture(graph, node_input):
    """Simple graph with a project node"""
    proj_a = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    return graph, proj_a
