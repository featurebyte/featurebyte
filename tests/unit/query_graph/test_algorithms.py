"""
Unit tests for featurebyte.query_graph.algorithms
"""
import pytest

from featurebyte.query_graph.algorithms import dfs_traversal
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph


@pytest.fixture(name="graph", scope="function")
def query_graph():
    """Empty query graph fixture"""
    GlobalQueryGraph.clear()
    yield GlobalQueryGraph()


@pytest.fixture(name="query_graph_with_nodes")
def query_graph_with_nodes(graph):
    """Fixture of a query graph with a groupby operation"""
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


def test_dfs__1(query_graph_with_nodes):
    """Test case for DFS traversal"""
    node = query_graph_with_nodes.get_node_by_name("input_1")
    result = list(dfs_traversal(query_graph_with_nodes, node))
    traverse_sequence = [x.name for x in result]
    assert traverse_sequence == ["input_1"]


def test_dfs__2(query_graph_with_nodes):
    """Test case for DFS traversal"""
    node = query_graph_with_nodes.get_node_by_name("project_2")
    result = list(dfs_traversal(query_graph_with_nodes, node))
    traverse_sequence = [x.name for x in result]
    assert traverse_sequence == ["project_2", "input_1"]


def test_dfs__3(query_graph_with_nodes):
    """Test case for DFS traversal"""
    node = query_graph_with_nodes.get_node_by_name("groupby_1")
    result = list(dfs_traversal(query_graph_with_nodes, node))
    traverse_sequence = [x.name for x in result]
    assert traverse_sequence == [
        "groupby_1",
        "assign_1",
        "input_1",
        "add_1",
        "project_1",
        "project_2",
    ]
