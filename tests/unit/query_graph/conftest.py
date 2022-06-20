"""
Common test fixtures used across unit test directories related to query_graph
"""
import pytest

from featurebyte.query_graph.enum import NodeOutputType, NodeType


@pytest.fixture(name="query_graph_with_groupby")
def query_graph_with_groupby_fixture(graph):
    """Fixture of a query graph with a groupby operation"""
    # pylint: disable=R0801
    node_input = graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "columns": ["ts", "cust_id", "a", "b"],
            "timestamp": "ts",
            "dbtable": "event_table",
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    proj_a = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    proj_b = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["b"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    sum_node = graph.add_operation(
        node_type=NodeType.ADD,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_a, proj_b],
    )
    assign_node = graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "c"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input, sum_node],
    )
    graph.add_operation(
        node_type=NodeType.GROUPBY,
        node_params={
            "keys": ["cust_id"],
            "parent": "a",
            "agg_func": "avg",
            "time_modulo_frequency": 5,
            "frequency": 30,
            "blind_spot": 1,
            "timestamp": "ts",
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_node],
    )
    return graph
