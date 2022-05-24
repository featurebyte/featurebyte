"""
Unit test for execution graph
"""
import pytest

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import Node, QueryGraph


@pytest.fixture(scope="module")
def query_graph():
    QueryGraph.clear()
    yield QueryGraph()


@pytest.fixture(scope="module")
def query_graph_single_node(query_graph):
    node_input = query_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    assert query_graph.to_dict() == {
        "nodes": {"input_1": {"type": "input", "parameters": {}, "output_type": "frame"}},
        "edges": {},
    }
    assert node_input == Node(name="input_1", type="input", parameters={}, output_type="frame")
    yield query_graph, node_input


@pytest.fixture(scope="module")
def query_graph_two_nodes(query_graph_single_node):
    query_graph, node_input = query_graph_single_node
    node_proj = query_graph.add_operation(
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
    assert query_graph.to_dict() == expected_graph
    assert node_proj == Node(
        name="project_1", type="project", parameters={"columns": ["a"]}, output_type="series"
    )
    yield query_graph, node_input, node_proj


@pytest.fixture(scope="module")
def query_graph_three_nodes(query_graph_two_nodes):
    query_graph, node_input, node_proj = query_graph_two_nodes
    node_eq = query_graph.add_operation(
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
    assert query_graph.to_dict() == expected_graph
    assert node_eq == Node(name="eq_1", type="eq", parameters={"value": 1}, output_type="series")
    yield query_graph, node_input, node_proj, node_eq


@pytest.fixture(scope="module")
def query_graph_four_nodes(query_graph_three_nodes):
    query_graph, node_input, node_proj, node_eq = query_graph_three_nodes
    node_filter = query_graph.add_operation(
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
    assert query_graph.to_dict() == expected_graph
    assert node_filter == Node(name="filter_1", type="filter", parameters={}, output_type="frame")
    yield query_graph, node_input, node_proj, node_eq, node_filter


def test_add_operation__add_duplicated_node_on_two_nodes_graph(query_graph_two_nodes):
    query_graph, node_input, node_proj = query_graph_two_nodes
    node_duplicated = query_graph.add_operation(
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
    assert query_graph.to_dict() == expected_graph
    assert node_duplicated == node_proj


def test_add_operation__add_duplicated_node_on_four_nodes_graph(query_graph_four_nodes):
    query_graph, _, node_proj, node_eq, _ = query_graph_four_nodes
    node_duplicated = query_graph.add_operation(
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
    assert query_graph.to_dict() == expected_graph
    assert node_duplicated == node_eq
