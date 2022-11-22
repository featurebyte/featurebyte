"""
Tests for nested graph related logic
"""
import pytest

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GraphNode, QueryGraph


@pytest.fixture(name="input_node_params")
def input_node_params_fixture():
    """Input node parameters fixture"""
    return {
        "type": "generic",
        "columns": ["col_int", "col_float", "col_varchar"],
        "table_details": {
            "database_name": "db",
            "schema_name": "public",
            "table_name": "transaction",
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


def test_graph_node_create__empty_input_nodes(input_node_params):
    """Test graph node: when input node is empty"""
    graph_node, nested_input_nodes = GraphNode.create(
        node_type=NodeType.INPUT,
        node_params=input_node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    expected_nested_input_node = {
        "name": "input_1",
        "type": "input",
        "parameters": {**input_node_params, "id": None},
        "output_type": "frame",
    }
    assert nested_input_nodes == []
    assert graph_node.output_node == expected_nested_input_node
    assert graph_node.parameters.graph == {"nodes": [expected_nested_input_node], "edges": []}
    assert graph_node.parameters.node_name_map == {}
    assert graph_node.parameters.output_node_name == "input_1"

    # test further operate on the graph node
    project_node = graph_node.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["col_int"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[graph_node.output_node],
    )
    assert graph_node.output_node == project_node
    assert graph_node.parameters.graph == {
        "nodes": [expected_nested_input_node, project_node],
        "edges": [{"source": "input_1", "target": project_node.name}],
    }
    assert graph_node.parameters.node_name_map == {}


def test_graph_node_create__non_empty_input_nodes(input_node_params):
    """Test graph node: when input node is non-empty"""
    graph = QueryGraph()
    input_node = graph.add_operation(
        node_type=NodeType.INPUT,
        node_params=input_node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    proj_int_node = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["col_int"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[input_node],
    )
    proj_float_node = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["col_float"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[input_node],
    )
    graph_node, nested_input_nodes = GraphNode.create(
        node_type=NodeType.ADD,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_int_node, proj_float_node],
    )
    expected_proxy_nodes = [
        {
            "name": f"proxy_input_{i+1}",
            "type": "proxy_input",
            "parameters": {"node_name": f"project_{i+1}"},
            "output_type": "series",
        }
        for i in range(2)
    ]
    assert nested_input_nodes == expected_proxy_nodes
    expected_nested_node = {
        "name": "add_1",
        "type": "add",
        "output_type": "series",
        "parameters": {"value": None},
    }
    assert graph_node.output_node == expected_nested_node
    assert graph_node.parameters.graph == {
        "nodes": expected_proxy_nodes + [expected_nested_node],
        "edges": [
            {"source": "proxy_input_1", "target": "add_1"},
            {"source": "proxy_input_2", "target": "add_1"},
        ],
    }
    assert graph_node.parameters.node_name_map == {
        "proxy_input_1": "project_1",
        "proxy_input_2": "project_2",
    }
    assert graph_node.parameters.output_node_name == "add_1"


@pytest.fixture(name="nested_input_graph")
def nested_input_graph_fixture(input_node_params):
    """
    Nested graph (graph node at input) fixture
    [[input] -> [project]] -> [add]
    """
    graph = QueryGraph()
    # construct nested graph through group node
    graph_node, _ = GraphNode.create(
        node_type=NodeType.INPUT,
        node_params=input_node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    project_node = graph_node.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["col_int"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[graph_node.output_node],  # graph_node.output_node: nested input node
    )
    assert graph_node.output_node == project_node
    inserted_graph_node = graph.add_graph_node(graph_node=graph_node, input_nodes=[])
    graph.add_operation(
        node_type=NodeType.ADD,
        node_params={"value": 10},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[inserted_graph_node],
    )
    assert graph.edges == [{"source": "graph_1", "target": "add_1"}]
    return graph


@pytest.fixture(name="nested_output_graph")
def nested_output_graph_fixture(input_node_params):
    """
    Nested graph (graph node at output)
    [input] -> [[project] -> [add]]
    """
    graph = QueryGraph()
    input_node = graph.add_operation(
        node_type=NodeType.INPUT,
        node_params=input_node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    graph_node, _ = GraphNode.create(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["col_int"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[input_node],
    )
    graph_node.add_operation(
        node_type=NodeType.ADD,
        node_params={"value": 10},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[graph_node.output_node],  # graph_node.output_node: nested project node
    )
    graph.add_graph_node(graph_node=graph_node, input_nodes=[input_node])
    assert graph.edges == [{"source": "input_1", "target": "graph_1"}]
    return graph


@pytest.fixture(name="deep_nested_graph")
def deep_nested_graph_fixture(input_node_params):
    """
    Deep nested graph
    [[[[input]] -> [project]] -> [add]]
    """
    graph = QueryGraph()
    deepest_graph_node, _ = GraphNode.create(
        node_type=NodeType.INPUT,
        node_params=input_node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    inner_graph_node, _ = GraphNode.create(
        node_type=NodeType.GRAPH,
        node_params=deepest_graph_node.parameters.dict(),
        node_output_type=deepest_graph_node.output_type,
        input_nodes=[],
    )
    inner_graph_node.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["col_int"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[
            inner_graph_node.output_node
        ],  # inner_graph_node.output_node: nested input node
    )
    graph_node, _ = GraphNode.create(
        node_type=NodeType.GRAPH,
        node_params=inner_graph_node.parameters.dict(),
        node_output_type=inner_graph_node.output_type,
        input_nodes=[],
    )
    graph_node.add_operation(
        node_type=NodeType.ADD,
        node_params={"value": 10},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[graph_node.output_node],  # graph_node.output_node: nested project node
    )
    graph.add_graph_node(graph_node, input_nodes=[])
    assert graph.edges == []
    inserted_inner_graph = graph.nodes[0].parameters.graph
    inserted_deeper_graph = inserted_inner_graph.nodes[0].parameters.graph
    inserted_deepest_graph = inserted_deeper_graph.nodes[0].parameters.graph
    assert inserted_inner_graph.edges == [{"source": "graph_1", "target": "add_1"}]
    assert inserted_deeper_graph.edges == [{"source": "graph_1", "target": "project_1"}]
    assert inserted_deepest_graph.edges == []
    return graph


def test_flatten_nested_graph(
    nested_input_graph, nested_output_graph, deep_nested_graph, input_node_params
):
    """Test query graph flatten logic"""
    expected_flattened_graph = {
        "edges": [
            {"source": "input_1", "target": "project_1"},
            {"source": "project_1", "target": "add_1"},
        ],
        "nodes": [
            {
                "name": "input_1",
                "type": "input",
                "output_type": "frame",
                "parameters": {**input_node_params, "id": None},
            },
            {
                "name": "project_1",
                "type": "project",
                "output_type": "series",
                "parameters": {"columns": ["col_int"]},
            },
            {
                "name": "add_1",
                "type": "add",
                "output_type": "series",
                "parameters": {"value": 10},
            },
        ],
    }
    assert nested_input_graph.flatten() == expected_flattened_graph
    assert nested_output_graph.flatten() == expected_flattened_graph
    assert deep_nested_graph.flatten() == expected_flattened_graph
