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
    node_input = execution_graph.add_operation(node_type="input", node_params={}, input_nodes=[])
    assert execution_graph.to_dict() == {
        "nodes": {"input_1": {"type": "input", "parameters": {}}},
        "edges": {},
    }
    assert node_input == Node(id="input_1", type="input", parameters={})
    yield execution_graph, node_input


@pytest.fixture(scope="module")
def execution_graph_two_nodes(execution_graph_single_node):
    execution_graph, node_input = execution_graph_single_node
    node_proj = execution_graph.add_operation(
        node_type="project", node_params={"columns": ["a"]}, input_nodes=[node_input]
    )
    expected_graph = {
        "nodes": {
            "input_1": {"type": "input", "parameters": {}},
            "project_1": {"type": "project", "parameters": {"columns": ["a"]}},
        },
        "edges": {"input_1": ["project_1"]},
    }
    assert execution_graph.to_dict() == expected_graph
    yield execution_graph, node_input, node_proj


@pytest.fixture(scope="module")
def execution_graph_three_nodes(execution_graph_two_nodes):
    execution_graph, node_input, node_proj = execution_graph_two_nodes
    node_eq = execution_graph.add_operation(
        node_type="equal", node_params={"value": 1}, input_nodes=[node_proj]
    )
    expected_graph = {
        "nodes": {
            "input_1": {"type": "input", "parameters": {}},
            "project_1": {"type": "project", "parameters": {"columns": ["a"]}},
            "equal_1": {"type": "equal", "parameters": {"value": 1}},
        },
        "edges": {
            "input_1": ["project_1"],
            "project_1": ["equal_1"],
        },
    }
    assert execution_graph.to_dict() == expected_graph
    yield execution_graph, node_input, node_proj, node_eq


@pytest.fixture(scope="module")
def execution_graph_four_nodes(execution_graph_three_nodes):
    execution_graph, node_input, node_proj, node_eq = execution_graph_three_nodes
    node_filter = execution_graph.add_operation(
        node_type="filter", node_params={}, input_nodes=[node_input, node_eq]
    )
    expected_graph = {
        "nodes": {
            "input_1": {"type": "input", "parameters": {}},
            "project_1": {"type": "project", "parameters": {"columns": ["a"]}},
            "equal_1": {"type": "equal", "parameters": {"value": 1}},
            "filter_1": {"type": "filter", "parameters": {}},
        },
        "edges": {
            "input_1": ["project_1", "filter_1"],
            "project_1": ["equal_1"],
            "equal_1": ["filter_1"],
        },
    }
    assert execution_graph.to_dict() == expected_graph
    yield execution_graph, node_input, node_proj, node_eq, node_filter


def test_add_operation__add_duplicated_node_on_two_nodes_graph(execution_graph_two_nodes):
    execution_graph, node_input, node_proj = execution_graph_two_nodes
    node_duplicated = execution_graph.add_operation(
        node_type="project", node_params={"columns": ["a"]}, input_nodes=[node_input]
    )
    expected_graph = {
        "nodes": {
            "input_1": {"type": "input", "parameters": {}},
            "project_1": {"type": "project", "parameters": {"columns": ["a"]}},
        },
        "edges": {"input_1": ["project_1"]},
    }
    assert execution_graph.to_dict() == expected_graph
    assert node_duplicated == node_proj


def test_add_operation__add_duplicated_node_on_four_nodes_graph(execution_graph_four_nodes):
    execution_graph, _, node_proj, node_eq, _ = execution_graph_four_nodes
    node_duplicated = execution_graph.add_operation(
        node_type="equal", node_params={"value": 1}, input_nodes=[node_proj]
    )
    expected_graph = {
        "nodes": {
            "input_1": {"type": "input", "parameters": {}},
            "project_1": {"type": "project", "parameters": {"columns": ["a"]}},
            "equal_1": {"type": "equal", "parameters": {"value": 1}},
            "filter_1": {"type": "filter", "parameters": {}},
        },
        "edges": {
            "input_1": ["project_1", "filter_1"],
            "project_1": ["equal_1"],
            "equal_1": ["filter_1"],
        },
    }
    assert execution_graph.to_dict() == expected_graph
    assert node_duplicated == node_eq
