"""
Tests for nested graph related logic
"""
import os.path

import pytest
from bson import json_util

from featurebyte.query_graph.enum import GraphNodeType, NodeOutputType, NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.graph_node.base import GraphNode


@pytest.fixture(name="input_node_params")
def input_node_params_fixture():
    """Input node parameters fixture"""
    return {
        "type": "generic",
        "columns": [
            {"name": "col_int", "dtype": "INT"},
            {"name": "col_float", "dtype": "FLOAT"},
            {"name": "col_varchar", "dtype": "VARCHAR"},
        ],
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
        graph_node_type=GraphNodeType.CLEANING,
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
        graph_node_type=GraphNodeType.CLEANING,
    )
    expected_proxy_nodes = [
        {
            "name": f"proxy_input_{i+1}",
            "type": "proxy_input",
            "parameters": {"input_order": i},
            "output_type": "series",
        }
        for i in range(2)
    ]
    assert nested_input_nodes == expected_proxy_nodes
    expected_nested_node = {
        "name": "add_1",
        "type": "add",
        "output_type": "series",
        "parameters": {"value": None, "right_op": False},
    }
    assert graph_node.output_node == expected_nested_node
    assert graph_node.parameters.graph == {
        "nodes": expected_proxy_nodes + [expected_nested_node],
        "edges": [
            {"source": "proxy_input_1", "target": "add_1"},
            {"source": "proxy_input_2", "target": "add_1"},
        ],
    }
    assert graph_node.parameters.output_node_name == "add_1"

    # insert graph node into the graph & check operation structure output
    inserted_graph_node = graph.add_node(
        node=graph_node, input_nodes=[proj_int_node, proj_float_node]
    )
    operation_structure = graph.extract_operation_structure(node=inserted_graph_node)
    # internal node names should not be included (node_names: add_1)
    assert operation_structure.dict() == {
        "aggregations": [],
        "columns": [
            {
                "columns": [
                    {
                        "filter": False,
                        "name": "col_int",
                        "node_names": {"project_1", "input_1"},
                        "node_name": "input_1",
                        "tabular_data_id": None,
                        "tabular_data_type": "generic",
                        "type": "source",
                        "dtype": "INT",
                    },
                    {
                        "filter": False,
                        "name": "col_float",
                        "node_names": {"input_1", "project_2"},
                        "node_name": "input_1",
                        "tabular_data_id": None,
                        "tabular_data_type": "generic",
                        "type": "source",
                        "dtype": "FLOAT",
                    },
                ],
                "filter": False,
                "name": None,
                "node_names": {"project_1", "graph_1", "input_1", "project_2"},
                "node_name": "graph_1",
                "transforms": ["graph"],
                "type": "derived",
                "dtype": "FLOAT",
            }
        ],
        "output_category": "view",
        "output_type": "series",
        "row_index_lineage": ("input_1",),
        "is_time_based": False,
    }
    # check neither node nor edge is pruned
    pruned_graph, node_name_map = graph.prune(target_node=inserted_graph_node, aggressive=True)
    assert len(pruned_graph.nodes) == len(graph.nodes)
    assert len(pruned_graph.edges) == len(graph.edges)


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
        graph_node_type=GraphNodeType.CLEANING,
    )
    project_node = graph_node.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["col_int"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[graph_node.output_node],  # graph_node.output_node: nested input node
    )
    assert graph_node.output_node == project_node
    inserted_graph_node = graph.add_node(node=graph_node, input_nodes=[])
    add_node = graph.add_operation(
        node_type=NodeType.ADD,
        node_params={"value": 10},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[inserted_graph_node],
    )
    assert graph.edges == [{"source": "graph_1", "target": "add_1"}]

    # internal node names should not be included (node_names: input_1, project_1)
    operation_structure = graph.extract_operation_structure(node=add_node)
    assert operation_structure.dict() == {
        "aggregations": [],
        "columns": [
            {
                "columns": [
                    {
                        "filter": False,
                        "name": "col_int",
                        "node_names": {"graph_1"},
                        "node_name": "graph_1",
                        "tabular_data_id": None,
                        "tabular_data_type": "generic",
                        "type": "source",
                        "dtype": "INT",
                    }
                ],
                "filter": False,
                "name": None,
                "node_names": {"add_1", "graph_1"},
                "node_name": "add_1",
                "transforms": ["add(value=10)"],
                "type": "derived",
                "dtype": "INT",
            }
        ],
        "output_category": "view",
        "output_type": "series",
        "row_index_lineage": ("input_1",),
        "is_time_based": False,
    }
    # check graph pruning
    pruned_graph, node_name_map = graph.prune(target_node=add_node, aggressive=True)
    assert pruned_graph == graph
    assert all(from_name == to_name for from_name, to_name in node_name_map.items())
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
        graph_node_type=GraphNodeType.CLEANING,
    )
    graph_node.add_operation(
        node_type=NodeType.ADD,
        node_params={"value": 10},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[graph_node.output_node],  # graph_node.output_node: nested project node
    )
    inserted_graph_node = graph.add_node(node=graph_node, input_nodes=[input_node])
    assert graph.edges == [{"source": "input_1", "target": "graph_1"}]

    # internal node names should not be included (node_names: project_1, add_1)
    operation_structure = graph.extract_operation_structure(node=inserted_graph_node)
    assert operation_structure.dict() == {
        "aggregations": [],
        "columns": [
            {
                "columns": [
                    {
                        "filter": False,
                        "name": "col_int",
                        "node_names": {"input_1", "graph_1"},
                        "node_name": "graph_1",
                        "tabular_data_id": None,
                        "tabular_data_type": "generic",
                        "type": "source",
                        "dtype": "INT",
                    }
                ],
                "filter": False,
                "name": None,
                "node_names": {"graph_1", "input_1"},
                "node_name": "graph_1",
                "transforms": ["graph"],
                "type": "derived",
                "dtype": "INT",
            }
        ],
        "output_category": "view",
        "output_type": "series",
        "row_index_lineage": ("input_1",),
        "is_time_based": False,
    }
    # check graph pruning
    pruned_graph, node_name_map = graph.prune(target_node=inserted_graph_node, aggressive=True)
    assert pruned_graph == graph
    assert all(from_name == to_name for from_name, to_name in node_name_map.items())
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
        graph_node_type=GraphNodeType.CLEANING,
    )
    inner_graph_node, _ = GraphNode.create(
        node_type=NodeType.GRAPH,
        node_params=deepest_graph_node.parameters.dict(),
        node_output_type=deepest_graph_node.output_type,
        input_nodes=[],
        graph_node_type=GraphNodeType.CLEANING,
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
        graph_node_type=GraphNodeType.CLEANING,
    )
    graph_node.add_operation(
        node_type=NodeType.ADD,
        node_params={"value": 10},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[graph_node.output_node],  # graph_node.output_node: nested project node
    )
    inserted_graph_node = graph.add_node(graph_node, input_nodes=[])
    assert graph.edges == []
    inserted_inner_graph = graph.nodes[0].parameters.graph
    inserted_deeper_graph = inserted_inner_graph.nodes[0].parameters.graph
    inserted_deepest_graph = inserted_deeper_graph.nodes[0].parameters.graph
    assert inserted_inner_graph.edges == [{"source": "graph_1", "target": "add_1"}]
    assert inserted_deeper_graph.edges == [{"source": "graph_1", "target": "project_1"}]
    assert inserted_deepest_graph.edges == []

    # internal node names should not be included (node_names: project_1, add_1)
    operation_structure = graph.extract_operation_structure(node=inserted_graph_node)
    assert operation_structure == {
        "aggregations": [],
        "columns": [
            {
                "columns": [
                    {
                        "filter": False,
                        "name": "col_int",
                        "node_names": {"graph_1"},
                        "node_name": "graph_1",
                        "tabular_data_id": None,
                        "tabular_data_type": "generic",
                        "type": "source",
                        "dtype": "INT",
                    }
                ],
                "filter": False,
                "name": None,
                "node_names": {"graph_1"},
                "node_name": "graph_1",
                "transforms": ["graph"],
                "type": "derived",
                "dtype": "INT",
            }
        ],
        "output_category": "view",
        "output_type": "series",
        "row_index_lineage": ("input_1",),
        "is_time_based": False,
    }
    # check graph pruning
    pruned_graph, node_name_map = graph.prune(target_node=inserted_graph_node, aggressive=True)
    assert pruned_graph == graph
    assert all(from_name == to_name for from_name, to_name in node_name_map.items())
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
                "parameters": {"value": 10, "right_op": False},
            },
        ],
    }
    assert nested_input_graph.flatten()[0] == expected_flattened_graph
    assert nested_output_graph.flatten()[0] == expected_flattened_graph
    assert deep_nested_graph.flatten()[0] == expected_flattened_graph


def test_nested_graph_pruning(input_details, groupby_node_params):
    """
    Test graph pruning on nested graph
    """

    def add_graph_node(query_graph, input_nodes):
        # construct a graph node that add a "a_plus_b" column (redundant column) to the input table
        # and generate a feature group
        graph_node, proxy_inputs = GraphNode.create(
            node_type=NodeType.PROJECT,
            node_params={"columns": ["a"]},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=input_nodes,
            graph_node_type=GraphNodeType.CLEANING,
        )
        node_proj_a = graph_node.output_node
        node_proj_b = graph_node.add_operation(
            node_type=NodeType.PROJECT,
            node_params={"columns": ["b"]},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=proxy_inputs,
        )
        node_add = graph_node.add_operation(
            node_type=NodeType.ADD,
            node_params={},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[node_proj_a, node_proj_b],  # graph_node.output_node: nested project node
        )
        node_assign = graph_node.add_operation(
            node_type=NodeType.ASSIGN,
            node_params={"name": "a_plus_b"},
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[proxy_inputs[0], node_add],
        )
        graph_node.add_operation(
            node_type=NodeType.GROUPBY,
            node_params=groupby_node_params,
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[node_assign],
        )
        return query_graph.add_node(graph_node, input_nodes)

    # construct a graph with a nested graph
    # [input] -> [graph] -> [project]
    graph = QueryGraph()
    input_node = graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "type": "event_data",
            "columns": [
                {"name": "ts", "dtype": "TIMESTAMP"},
                {"name": "cust_id", "dtype": "INT"},
                {"name": "a", "dtype": "FLOAT"},
                {"name": "b", "dtype": "FLOAT"},
            ],
            "timestamp": "ts",
            **input_details,
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    node_graph = add_graph_node(query_graph=graph, input_nodes=[input_node])
    node_proj_2h_avg = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a_2h_average"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_graph],
    )

    # check operation structure
    operation_structure = graph.extract_operation_structure(node=node_proj_2h_avg)
    assert operation_structure.dict() == {
        "aggregations": [
            {
                "category": None,
                "column": {
                    "filter": False,
                    "name": "a",
                    "node_names": {"input_1"},
                    "node_name": "input_1",
                    "tabular_data_id": None,
                    "tabular_data_type": "event_data",
                    "type": "source",
                    "dtype": "FLOAT",
                },
                "filter": False,
                "keys": ["cust_id"],
                "aggregation_type": "groupby",
                "method": "avg",
                "name": "a_2h_average",
                "node_names": {"input_1", "graph_1", "project_1"},
                "node_name": "graph_1",
                "type": "aggregation",
                "window": "2h",
                "dtype": "FLOAT",
            }
        ],
        "columns": [
            {
                "filter": False,
                "name": "a",
                "node_names": {"input_1"},
                "node_name": "input_1",
                "tabular_data_id": None,
                "tabular_data_type": "event_data",
                "type": "source",
                "dtype": "FLOAT",
            }
        ],
        "output_category": "feature",
        "output_type": "series",
        "row_index_lineage": ("groupby_1",),
        "is_time_based": True,
    }

    # check pruned graph
    pruned_graph, node_name_map = graph.prune(target_node=node_proj_2h_avg, aggressive=True)
    assert node_name_map == {"input_1": "input_1", "graph_1": "graph_1", "project_1": "project_1"}
    assert pruned_graph.edges_map == {"input_1": ["graph_1"], "graph_1": ["project_1"]}

    # check nested graph edges (note that assign node is pruned)
    nested_graph = pruned_graph.nodes_map["graph_1"].parameters.graph
    assert nested_graph.edges_map == {"proxy_input_1": ["groupby_1"]}
    assert nested_graph.get_node_by_name("groupby_1").dict() == {
        "name": "groupby_1",
        "output_type": "frame",
        "parameters": {
            "agg_func": "avg",
            "aggregation_id": None,
            "blind_spot": 900,
            "entity_ids": groupby_node_params["entity_ids"],
            "frequency": 3600,
            "keys": ["cust_id"],
            "names": ["a_2h_average"],  # before pruned: ["a_2h_average", "a_48h_average"]
            "parent": "a",
            "serving_names": ["CUSTOMER_ID"],
            "tile_id": None,
            "time_modulo_frequency": 1800,
            "timestamp": "ts",
            "value_by": None,
            "windows": ["2h"],
        },
        "type": "groupby",
    }


def test_graph_node__redundant_graph_node(input_node_params):
    """Test graph node (redundant graph node)"""

    def add_graph_node(query_graph, input_nodes):
        # construct a graph node which contains a single node (ASSIGN node)
        node_graph, proxy_inputs = GraphNode.create(
            node_type=NodeType.ASSIGN,
            node_params={"name": "col_int_plus_one"},
            node_output_type=NodeOutputType.FRAME,
            input_nodes=input_nodes,
            graph_node_type=GraphNodeType.CLEANING,
        )
        return query_graph.add_node(
            node=node_graph,
            input_nodes=input_nodes,
        )

    graph = QueryGraph()
    input_node = graph.add_operation(
        node_type=NodeType.INPUT,
        node_params=input_node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    proj_col_int_node = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["col_int"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[input_node],
    )
    add_node = graph.add_operation(
        node_type=NodeType.ADD,
        node_params={"value": 1},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_col_int_node],  # graph_node.output_node: nested project node
    )
    graph_node = add_graph_node(query_graph=graph, input_nodes=[input_node, add_node])
    proj_node = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["col_int"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[graph_node],
    )
    operation_structure = graph.extract_operation_structure(node=proj_node)
    assert operation_structure.dict() == {
        "aggregations": [],
        "columns": [
            {
                "filter": False,
                "name": "col_int",
                "node_names": {"project_2", "input_1"},
                "node_name": "input_1",
                "tabular_data_id": None,
                "tabular_data_type": "generic",
                "type": "source",
                "dtype": "INT",
            }
        ],
        "output_category": "view",
        "output_type": "series",
        "row_index_lineage": ("input_1",),
        "is_time_based": False,
    }
    # TODO: [DEV-868] Make graph node prunable
    pruned_graph, node_name_map = graph.prune(target_node=proj_node, aggressive=True)
    assert pruned_graph.edges_map == {
        "input_1": ["project_1", "graph_1"],
        "project_1": ["add_1"],
        "add_1": ["graph_1"],
        "graph_1": ["project_2"],
    }
    nested_graph_nodes = pruned_graph.get_node_by_name("graph_1").parameters.graph.nodes
    assert nested_graph_nodes == [
        # note that second proxy input node is pruned
        {
            "name": "proxy_input_1",
            "type": "proxy_input",
            "output_type": "frame",
            "parameters": {"input_order": 0},
        }
    ]


def test_graph_flattening(test_dir):
    """Test graph flattening"""
    fixture_path = os.path.join(test_dir, "fixtures/graph/event_view_nested_graph.json")
    with open(fixture_path, "r") as file_handle:
        query_graph_dict = json_util.loads(file_handle.read())
        query_graph = QueryGraph(**query_graph_dict)

    # the graph only contains 2 nodes (input node and graph node)
    assert list(query_graph.nodes_map.keys()) == ["input_1", "graph_1"]

    # check the flattened graph & node name map
    flattened_graph, node_name_map = query_graph.flatten()
    assert node_name_map == {"input_1": "input_1", "graph_1": "project_3"}
    output_node = flattened_graph.get_node_by_name("project_3")
    assert output_node.parameters.columns == [
        "col_int",
        "col_float",
        "col_char",
        "col_text",
        "col_binary",
        "col_boolean",
        "event_timestamp",
        "created_at",
        "cust_id",
    ]
