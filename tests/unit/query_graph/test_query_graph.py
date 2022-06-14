"""
Unit test for query graph
"""
import pytest

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import Node, QueryGraph


@pytest.fixture(name="graph")
def query_graph():
    """
    Empty query graph fixture
    """
    QueryGraph.clear()
    yield QueryGraph()


@pytest.fixture(name="graph_single_node")
def query_graph_single_node(graph):
    """
    Query graph with a single node
    """
    node_input = graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    assert graph.to_dict() == {
        "nodes": {
            "input_1": {
                "name": "input_1",
                "type": "input",
                "parameters": {},
                "output_type": "frame",
            }
        },
        "edges": {},
    }
    assert node_input == Node(name="input_1", type="input", parameters={}, output_type="frame")
    yield graph, node_input


@pytest.fixture(name="graph_two_nodes")
def query_graph_two_nodes(graph_single_node):
    """
    Query graph with two nodes
    """
    graph, node_input = graph_single_node
    node_proj = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    expected_graph = {
        "nodes": {
            "input_1": {"type": "input", "parameters": {}, "output_type": "frame"},
            "project_1": {
                "type": "project",
                "parameters": {"columns": ["a"]},
                "output_type": "series",
            },
        },
        "edges": {"input_1": ["project_1"]},
    }
    assert graph.to_dict(exclude_name=True) == expected_graph
    assert node_proj == Node(
        name="project_1", type="project", parameters={"columns": ["a"]}, output_type="series"
    )
    yield graph, node_input, node_proj


@pytest.fixture(name="graph_three_nodes")
def query_graph_three_nodes(graph_two_nodes):
    """
    Query graph with three nodes
    """
    graph, node_input, node_proj = graph_two_nodes
    node_eq = graph.add_operation(
        node_type=NodeType.EQ,
        node_params={"value": 1},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_proj],
    )
    expected_graph = {
        "nodes": {
            "input_1": {"type": "input", "parameters": {}, "output_type": "frame"},
            "project_1": {
                "type": "project",
                "parameters": {"columns": ["a"]},
                "output_type": "series",
            },
            "eq_1": {"type": "eq", "parameters": {"value": 1}, "output_type": "series"},
        },
        "edges": {
            "input_1": ["project_1"],
            "project_1": ["eq_1"],
        },
    }
    assert graph.to_dict(exclude_name=True) == expected_graph
    assert node_eq == Node(name="eq_1", type="eq", parameters={"value": 1}, output_type="series")
    yield graph, node_input, node_proj, node_eq


@pytest.fixture(name="graph_four_nodes")
def query_graph_four_nodes(graph_three_nodes):
    """
    Query graph with four nodes
    """
    graph, node_input, node_proj, node_eq = graph_three_nodes
    node_filter = graph.add_operation(
        node_type=NodeType.FILTER,
        node_params={},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input, node_eq],
    )
    expected_graph = {
        "nodes": {
            "input_1": {"type": "input", "parameters": {}, "output_type": "frame"},
            "project_1": {
                "type": "project",
                "parameters": {"columns": ["a"]},
                "output_type": "series",
            },
            "eq_1": {"type": "eq", "parameters": {"value": 1}, "output_type": "series"},
            "filter_1": {"type": "filter", "parameters": {}, "output_type": "frame"},
        },
        "edges": {
            "input_1": ["project_1", "filter_1"],
            "project_1": ["eq_1"],
            "eq_1": ["filter_1"],
        },
    }
    assert graph.to_dict(exclude_name=True) == expected_graph
    assert node_filter == Node(name="filter_1", type="filter", parameters={}, output_type="frame")
    yield graph, node_input, node_proj, node_eq, node_filter


def test_add_operation__add_duplicated_node_on_two_nodes_graph(graph_two_nodes):
    """
    Test add operation by adding a duplicated node on a 2-node graph
    """
    graph, node_input, node_proj = graph_two_nodes
    node_duplicated = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    expected_graph = {
        "nodes": {
            "input_1": {"type": "input", "parameters": {}, "output_type": "frame"},
            "project_1": {
                "type": "project",
                "parameters": {"columns": ["a"]},
                "output_type": "series",
            },
        },
        "edges": {"input_1": ["project_1"]},
    }
    assert graph.to_dict(exclude_name=True) == expected_graph
    assert node_duplicated == node_proj


def test_add_operation__add_duplicated_node_on_four_nodes_graph(graph_four_nodes):
    """
    Test add operation by adding a duplicated node on a 4-node graph
    """
    graph, _, node_proj, node_eq, _ = graph_four_nodes
    node_duplicated = graph.add_operation(
        node_type=NodeType.EQ,
        node_params={"value": 1},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_proj],
    )
    expected_graph = {
        "nodes": {
            "input_1": {"type": "input", "parameters": {}, "output_type": "frame"},
            "project_1": {
                "type": "project",
                "parameters": {"columns": ["a"]},
                "output_type": "series",
            },
            "eq_1": {"type": "eq", "parameters": {"value": 1}, "output_type": "series"},
            "filter_1": {"type": "filter", "parameters": {}, "output_type": "frame"},
        },
        "edges": {
            "input_1": ["project_1", "filter_1"],
            "project_1": ["eq_1"],
            "eq_1": ["filter_1"],
        },
    }
    assert graph.to_dict(exclude_name=True) == expected_graph
    assert node_duplicated == node_eq
