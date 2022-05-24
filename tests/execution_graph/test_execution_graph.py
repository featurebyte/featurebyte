"""
Unit test for execution graph
"""
import pytest

from featurebyte.execution_graph.graph import ExecutionGraph, Node


@pytest.fixture(scope="module")
def execution_graph():
    yield ExecutionGraph()


@pytest.fixture(scope="module")
def execution_graph_single_node(execution_graph):
    node_input = execution_graph.add_operation(
        node_type="input", node_params={}, node_output_type="DataFrame", input_nodes=[]
    )
    assert execution_graph.to_dict() == {
        "nodes": {"input_1": {"type": "input", "parameters": {}, "output_type": "DataFrame"}},
        "edges": {},
    }
    assert node_input == Node(id="input_1", type="input", parameters={}, output_type="DataFrame")
    yield execution_graph, node_input


@pytest.fixture(scope="module")
def execution_graph_two_nodes(execution_graph_single_node):
    execution_graph, node_input = execution_graph_single_node
    node_proj = execution_graph.add_operation(
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
    assert execution_graph.to_dict() == expected_graph
    assert node_proj == Node(
        id="project_1", type="project", parameters={"columns": ["a"]}, output_type="Series"
    )
    yield execution_graph, node_input, node_proj


@pytest.fixture(scope="module")
def execution_graph_three_nodes(execution_graph_two_nodes):
    execution_graph, node_input, node_proj = execution_graph_two_nodes
    node_eq = execution_graph.add_operation(
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
    assert execution_graph.to_dict() == expected_graph
    assert node_eq == Node(
        id="equal_1", type="equal", parameters={"value": 1}, output_type="Series"
    )
    yield execution_graph, node_input, node_proj, node_eq


@pytest.fixture(scope="module")
def execution_graph_four_nodes(execution_graph_three_nodes):
    execution_graph, node_input, node_proj, node_eq = execution_graph_three_nodes
    node_filter = execution_graph.add_operation(
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
    assert execution_graph.to_dict() == expected_graph
    assert node_filter == Node(id="filter_1", type="filter", parameters={}, output_type="DataFrame")
    yield execution_graph, node_input, node_proj, node_eq, node_filter


def test_add_operation__add_duplicated_node_on_two_nodes_graph(execution_graph_two_nodes):
    execution_graph, node_input, node_proj = execution_graph_two_nodes
    node_duplicated = execution_graph.add_operation(
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
    assert execution_graph.to_dict() == expected_graph
    assert node_duplicated == node_proj


def test_add_operation__add_duplicated_node_on_four_nodes_graph(execution_graph_four_nodes):
    execution_graph, _, node_proj, node_eq, _ = execution_graph_four_nodes
    node_duplicated = execution_graph.add_operation(
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
    assert execution_graph.to_dict() == expected_graph
    assert node_duplicated == node_eq
