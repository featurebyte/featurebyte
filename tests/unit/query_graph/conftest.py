"""
Common test fixtures used across unit test directories related to query_graph
"""
import pytest

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import Node


@pytest.fixture(name="query_graph_with_groupby")
def query_graph_with_groupby_fixture(graph):
    """Fixture of a query graph with a groupby operation"""
    # pylint: disable=duplicate-code
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
            "windows": [86400],
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_node],
    )
    return graph


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
    pruned_graph, mapped_node = graph.prune(target_node=node_input, target_columns=set())
    assert mapped_node.name == "input_1"
    graph_dict = graph.dict()
    assert graph_dict == pruned_graph.dict()
    assert graph_dict["nodes"] == {
        "input_1": {
            "name": "input_1",
            "type": "input",
            "parameters": {},
            "output_type": "frame",
        }
    }
    assert graph_dict["edges"] == {}
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
    pruned_graph, mapped_node = graph.prune(target_node=node_proj, target_columns={"a"})
    assert mapped_node.name == "project_1"
    graph_dict = graph.dict()
    assert graph_dict == pruned_graph.dict()
    assert graph_dict["nodes"] == {
        "input_1": {"name": "input_1", "type": "input", "parameters": {}, "output_type": "frame"},
        "project_1": {
            "name": "project_1",
            "type": "project",
            "parameters": {"columns": ["a"]},
            "output_type": "series",
        },
    }
    assert graph_dict["edges"] == {"input_1": ["project_1"]}
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
    pruned_graph, mapped_node = graph.prune(target_node=node_eq, target_columns=set())
    assert mapped_node.name == "eq_1"
    graph_dict = graph.dict()
    assert graph_dict == pruned_graph.dict()
    assert graph_dict["nodes"] == {
        "input_1": {"name": "input_1", "type": "input", "parameters": {}, "output_type": "frame"},
        "project_1": {
            "name": "project_1",
            "type": "project",
            "parameters": {"columns": ["a"]},
            "output_type": "series",
        },
        "eq_1": {"name": "eq_1", "type": "eq", "parameters": {"value": 1}, "output_type": "series"},
    }
    assert graph_dict["edges"] == {"input_1": ["project_1"], "project_1": ["eq_1"]}
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
    pruned_graph, mapped_node = graph.prune(target_node=node_filter, target_columns=set())
    assert mapped_node.name == "filter_1"
    graph_dict = graph.dict()
    assert graph_dict == pruned_graph.dict()
    assert graph_dict["nodes"] == {
        "input_1": {"name": "input_1", "type": "input", "parameters": {}, "output_type": "frame"},
        "project_1": {
            "name": "project_1",
            "type": "project",
            "parameters": {"columns": ["a"]},
            "output_type": "series",
        },
        "eq_1": {"name": "eq_1", "type": "eq", "parameters": {"value": 1}, "output_type": "series"},
        "filter_1": {
            "name": "filter_1",
            "type": "filter",
            "parameters": {},
            "output_type": "frame",
        },
    }
    assert graph_dict["edges"] == {
        "input_1": ["project_1", "filter_1"],
        "project_1": ["eq_1"],
        "eq_1": ["filter_1"],
    }
    assert node_filter == Node(name="filter_1", type="filter", parameters={}, output_type="frame")
    yield graph, node_input, node_proj, node_eq, node_filter
