"""
Tests for nested graph pruning related logic
"""
from featurebyte.query_graph.enum import GraphNodeType, NodeOutputType, NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.graph_node.base import GraphNode
from tests.unit.query_graph.util import to_dict


def add_input_node(query_graph, input_details):
    # construct an input node
    return query_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "type": "event_table",
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


def add_graph_node(query_graph, input_nodes, groupby_node_params=None):
    # construct a graph node that add a "a_plus_b" column (redundant column) to the input table
    # if groupby_node_params is not None, then generate a feature group output
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
    if groupby_node_params:
        graph_node.add_operation(
            node_type=NodeType.GROUPBY,
            node_params=groupby_node_params,
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[node_assign],
        )
    else:
        graph_node.add_operation(
            node_type=NodeType.ASSIGN,
            node_params={"name": "a", "value": 100},
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[graph_node.output_node],
        )
    return query_graph.add_node(graph_node, input_nodes)


def test_nested_graph_pruning(input_details, groupby_node_params):
    """
    Test graph pruning on nested graph
    """
    # construct a graph with a nested graph
    # [input] -> [graph] -> [project]
    # for the nested graph node, the output node is a groupby node
    graph = QueryGraph()
    input_node = add_input_node(graph, input_details)
    node_graph = add_graph_node(
        query_graph=graph, input_nodes=[input_node], groupby_node_params=groupby_node_params
    )
    node_proj_2h_avg = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a_2h_average"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_graph],
    )

    # check operation structure
    operation_structure = graph.extract_operation_structure(
        node=node_proj_2h_avg, keep_all_source_columns=True
    )
    common_column_params = {
        "node_names": {"input_1"},
        "node_name": "input_1",
        "table_id": None,
        "table_type": "event_table",
        "type": "source",
        "filter": False,
    }
    assert to_dict(operation_structure) == {
        "aggregations": [
            {
                "category": None,
                "column": {
                    "filter": False,
                    "name": "a",
                    "node_names": {"input_1"},
                    "node_name": "input_1",
                    "table_id": None,
                    "table_type": "event_table",
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
            {"name": "ts", "dtype": "TIMESTAMP", **common_column_params},
            {"name": "cust_id", "dtype": "INT", **common_column_params},
            {"name": "a", "dtype": "FLOAT", **common_column_params},
        ],
        "output_category": "feature",
        "output_type": "series",
        "row_index_lineage": ("groupby_1",),
        "is_time_based": True,
    }

    # check pruned graph
    pruned_graph, node_name_map = graph.prune(target_node=node_proj_2h_avg)
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


def test_graph_node__when_graph_node_is_output_node(input_details):
    """Test graph node pruning -- when the graph node is the output node"""
    # construct a graph node with a nested graph
    # [input] -> [graph] -> [project]
    # for the nested graph node, the output node is a graph node
    graph = QueryGraph()
    input_node = add_input_node(graph, input_details)
    graph_node = add_graph_node(
        query_graph=graph, input_nodes=[input_node], groupby_node_params=None
    )
    proj_node = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[graph_node],
    )

    # check operation structure
    pruned_graph, node_name_map = graph.prune(target_node=proj_node)
    assert pruned_graph.edges_map == {"input_1": ["graph_1"], "graph_1": ["project_1"]}

    # check nested graph edges (check all the unused nested nodes get pruned)
    nested_graph = pruned_graph.nodes_map["graph_1"].parameters.graph
    assert nested_graph.edges_map == {"proxy_input_1": ["assign_1"]}
    assert nested_graph.nodes == [
        {
            "name": "proxy_input_1",
            "type": "proxy_input",
            "output_type": "frame",
            "parameters": {"input_order": 0},
        },
        {
            "name": "assign_1",
            "type": "assign",
            "output_type": "frame",
            "parameters": {"name": "a", "value": 100},
        },
    ]
