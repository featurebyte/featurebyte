"""
Unit test for execution graph
"""
import pytest

from featurebyte.query_graph.graph import Node, QueryGraph


@pytest.fixture(scope="module")
def query_graph():
    QueryGraph.clear()
    yield QueryGraph()


@pytest.fixture(scope="module")
def query_graph_single_node(query_graph):
    node_input = query_graph.add_operation(
        node_type="input", node_params={}, node_output_type="DataFrame", input_nodes=[]
    )
    assert query_graph.to_dict() == {
        "nodes": {"input_1": {"type": "input", "parameters": {}, "output_type": "DataFrame"}},
        "edges": {},
    }
    assert node_input == Node(id="input_1", type="input", parameters={}, output_type="DataFrame")
    yield query_graph, node_input


@pytest.fixture(scope="module")
def query_graph_two_nodes(query_graph_single_node):
    query_graph, node_input = query_graph_single_node
    node_proj = query_graph.add_operation(
        node_type="project",
        node_params={"columns": ["a"]},
        node_output_type="Series",
        input_nodes=[node_input],
    )
    expected_graph = {
        "nodes": {
            "input_1": {"type": "input", "parameters": {}, "output_type": "DataFrame"},
            "project_1": {
                "type": "project",
                "parameters": {"columns": ["a"]},
                "output_type": "Series",
            },
        },
        "edges": {"input_1": ["project_1"]},
    }
    assert query_graph.to_dict() == expected_graph
    assert node_proj == Node(
        id="project_1", type="project", parameters={"columns": ["a"]}, output_type="Series"
    )
    yield query_graph, node_input, node_proj


@pytest.fixture(scope="module")
def query_graph_three_nodes(query_graph_two_nodes):
    query_graph, node_input, node_proj = query_graph_two_nodes
    node_eq = query_graph.add_operation(
        node_type="equal",
        node_params={"value": 1},
        node_output_type="Series",
        input_nodes=[node_proj],
    )
    expected_graph = {
        "nodes": {
            "input_1": {"type": "input", "parameters": {}, "output_type": "DataFrame"},
            "project_1": {
                "type": "project",
                "parameters": {"columns": ["a"]},
                "output_type": "Series",
            },
            "equal_1": {"type": "equal", "parameters": {"value": 1}, "output_type": "Series"},
        },
        "edges": {
            "input_1": ["project_1"],
            "project_1": ["equal_1"],
        },
    }
    assert query_graph.to_dict() == expected_graph
    assert node_eq == Node(
        id="equal_1", type="equal", parameters={"value": 1}, output_type="Series"
    )
    yield query_graph, node_input, node_proj, node_eq


@pytest.fixture(scope="module")
def query_graph_four_nodes(query_graph_three_nodes):
    query_graph, node_input, node_proj, node_eq = query_graph_three_nodes
    node_filter = query_graph.add_operation(
        node_type="filter",
        node_params={},
        node_output_type="DataFrame",
        input_nodes=[node_input, node_eq],
    )
    expected_graph = {
        "nodes": {
            "input_1": {"type": "input", "parameters": {}, "output_type": "DataFrame"},
            "project_1": {
                "type": "project",
                "parameters": {"columns": ["a"]},
                "output_type": "Series",
            },
            "equal_1": {"type": "equal", "parameters": {"value": 1}, "output_type": "Series"},
            "filter_1": {"type": "filter", "parameters": {}, "output_type": "DataFrame"},
        },
        "edges": {
            "input_1": ["project_1", "filter_1"],
            "project_1": ["equal_1"],
            "equal_1": ["filter_1"],
        },
    }
    assert query_graph.to_dict() == expected_graph
    assert node_filter == Node(id="filter_1", type="filter", parameters={}, output_type="DataFrame")
    yield query_graph, node_input, node_proj, node_eq, node_filter


def test_add_operation__add_duplicated_node_on_two_nodes_graph(query_graph_two_nodes):
    query_graph, node_input, node_proj = query_graph_two_nodes
    node_duplicated = query_graph.add_operation(
        node_type="project",
        node_params={"columns": ["a"]},
        node_output_type="Series",
        input_nodes=[node_input],
    )
    expected_graph = {
        "nodes": {
            "input_1": {"type": "input", "parameters": {}, "output_type": "DataFrame"},
            "project_1": {
                "type": "project",
                "parameters": {"columns": ["a"]},
                "output_type": "Series",
            },
        },
        "edges": {"input_1": ["project_1"]},
    }
    assert query_graph.to_dict() == expected_graph
    assert node_duplicated == node_proj


def test_add_operation__add_duplicated_node_on_four_nodes_graph(query_graph_four_nodes):
    query_graph, _, node_proj, node_eq, _ = query_graph_four_nodes
    node_duplicated = query_graph.add_operation(
        node_type="equal",
        node_params={"value": 1},
        node_output_type="Series",
        input_nodes=[node_proj],
    )
    expected_graph = {
        "nodes": {
            "input_1": {"type": "input", "parameters": {}, "output_type": "DataFrame"},
            "project_1": {
                "type": "project",
                "parameters": {"columns": ["a"]},
                "output_type": "Series",
            },
            "equal_1": {"type": "equal", "parameters": {"value": 1}, "output_type": "Series"},
            "filter_1": {"type": "filter", "parameters": {}, "output_type": "DataFrame"},
        },
        "edges": {
            "input_1": ["project_1", "filter_1"],
            "project_1": ["equal_1"],
            "equal_1": ["filter_1"],
        },
    }
    assert query_graph.to_dict() == expected_graph
    assert node_duplicated == node_eq
