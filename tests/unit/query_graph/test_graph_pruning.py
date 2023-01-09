"""
Test for graph pruning related logics
"""
import os

from bson import json_util

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import construct_node
from tests.util.helper import add_groupby_operation


def test_prune__redundant_assign_nodes(dataframe):
    """
    Test graph pruning on a query graph with redundant assign nodes
    """
    dataframe["redundantA"] = dataframe["CUST_ID"] / 10
    dataframe["redundantB"] = dataframe["VALUE"] + 10
    dataframe["target"] = dataframe["CUST_ID"] * dataframe["VALUE"]
    assert dataframe.node == construct_node(
        name="assign_3", type="assign", parameters={"name": "target"}, output_type="frame"
    )
    target_node = dataframe["target"].node
    pruned_graph, node_name_map = dataframe.graph.prune(target_node=target_node, aggressive=True)
    mapped_node = pruned_graph.get_node_by_name(node_name_map[dataframe.node.name])
    assert pruned_graph.edges_map == {
        "assign_1": ["project_3"],
        "input_1": ["project_1", "project_2", "assign_1"],
        "project_1": ["mul_1"],
        "project_2": ["mul_1"],
        "mul_1": ["assign_1"],
    }
    assert pruned_graph.nodes_map["assign_1"] == {
        "name": "assign_1",
        "type": "assign",
        "parameters": {"name": "target", "value": None},
        "output_type": "frame",
    }
    assert mapped_node.name == "assign_1"


def test_prune__redundant_assign_node_with_same_target_column_name(dataframe):
    """
    Test graph pruning on a query graph with redundant assign node of same target name
    """
    dataframe["VALUE"] = 1
    dataframe["VALUE"] = dataframe["CUST_ID"] * 10
    # convert the dataframe into dictionary & compare some attribute values (non-aggressive pruning)
    pruned_graph, node_name_map = dataframe.graph.prune(target_node=dataframe.node, aggressive=True)
    assert pruned_graph.edges == [
        {"source": "input_1", "target": "project_1"},
        {"source": "project_1", "target": "mul_1"},
        {"source": "input_1", "target": "assign_1"},
        {"source": "mul_1", "target": "assign_1"},
    ]
    mapped_node = pruned_graph.get_node_by_name(node_name_map[dataframe.node.name])
    assert pruned_graph.nodes_map["assign_1"].parameters.dict() == {"name": "VALUE", "value": None}
    assert mapped_node.name == "assign_1"


def test_prune__redundant_project_nodes(dataframe):
    """
    Test graph pruning on a query graph with redundant project nodes
    """
    _ = dataframe["CUST_ID"]
    _ = dataframe["VALUE"]
    mask = dataframe["MASK"]
    pruned_graph, node_name_map = dataframe.graph.prune(target_node=mask.node)
    mapped_node = pruned_graph.get_node_by_name(node_name_map[mask.node.name])
    assert pruned_graph.edges_map == {"input_1": ["project_1"]}
    assert pruned_graph.nodes_map["project_1"].parameters.columns == ["MASK"]
    assert mapped_node.name == "project_1"


def test_prune__multiple_non_redundant_assign_nodes__interactive_pattern(dataframe):
    """
    Test graph pruning on a query graph without any redundant assign nodes (interactive pattern)
    """
    dataframe["requiredA"] = dataframe["CUST_ID"] / 10
    dataframe["requiredB"] = dataframe["VALUE"] + 10
    dataframe["target"] = dataframe["requiredA"] * dataframe["requiredB"]
    target_node = dataframe["target"].node
    pruned_graph, node_name_map = dataframe.graph.prune(target_node=target_node)
    assert pruned_graph.edges_map == {
        "input_1": ["project_1", "assign_1", "project_2"],
        "project_1": ["div_1"],
        "div_1": ["assign_1"],
        "project_2": ["add_1"],
        "add_1": ["assign_2"],
        "assign_1": ["assign_2", "project_4"],
        "assign_2": ["project_3", "assign_3"],
        "project_3": ["mul_1"],
        "project_4": ["mul_1"],
        "mul_1": ["assign_3"],
        "assign_3": ["project_5"],
    }
    assert pruned_graph.nodes_map["assign_1"].parameters.name == "requiredA"
    assert pruned_graph.nodes_map["assign_2"].parameters.name == "requiredB"
    assert pruned_graph.nodes_map["project_1"].parameters.columns == ["CUST_ID"]
    assert pruned_graph.nodes_map["project_2"].parameters.columns == ["VALUE"]
    assert pruned_graph.nodes_map["project_3"].parameters.columns == ["requiredB"]
    assert pruned_graph.nodes_map["project_4"].parameters.columns == ["requiredA"]
    assert pruned_graph.nodes_map["project_5"].parameters.columns == ["target"]
    mapped_node = pruned_graph.get_node_by_name(node_name_map[target_node.name])
    assert mapped_node.name == "project_5"


def test_prune__multiple_non_redundant_assign_nodes__cascading_pattern(dataframe):
    """
    Test graph pruning on a query graph without any redundant assign nodes (cascading pattern)
    """
    dataframe["requiredA"] = dataframe["CUST_ID"] / 10
    dataframe["requiredB"] = dataframe["requiredA"] + 10
    dataframe["target"] = dataframe["requiredB"] * 10
    pruned_graph, node_name_map = dataframe.graph.prune(target_node=dataframe.node)
    mapped_node = pruned_graph.get_node_by_name(node_name_map[dataframe.node.name])
    assert pruned_graph.edges_map == {
        "input_1": ["project_1", "assign_1"],
        "project_1": ["div_1"],
        "div_1": ["assign_1"],
        "assign_1": ["project_2", "assign_2"],
        "project_2": ["add_1"],
        "add_1": ["assign_2"],
        "assign_2": ["project_3", "assign_3"],
        "project_3": ["mul_1"],
        "mul_1": ["assign_3"],
    }
    assert pruned_graph.nodes_map["assign_1"].parameters.name == "requiredA"
    assert pruned_graph.nodes_map["assign_2"].parameters.name == "requiredB"
    assert pruned_graph.nodes_map["project_1"].parameters.columns == ["CUST_ID"]
    assert pruned_graph.nodes_map["project_2"].parameters.columns == ["requiredA"]
    assert pruned_graph.nodes_map["project_3"].parameters.columns == ["requiredB"]
    assert mapped_node.name == "assign_3"


def test_prune__item_view_join_event_view(test_dir):
    """Test graph pruning on item view join with event view"""
    fixture_path = os.path.join(test_dir, "fixtures/graph/event_item_view_join.json")
    with open(fixture_path) as fhandle:
        graph_dict = json_util.loads(fhandle.read())

    query_graph = QueryGraph(**graph_dict)
    assert "assign_1" in query_graph.nodes_map

    # check that assign node not get pruned
    target_node = query_graph.get_node_by_name("join_2")
    pruned_graph, _ = query_graph.prune(target_node=target_node)
    assert "assign_1" in pruned_graph.nodes_map


def test_join_feature_node_is_prunable(global_graph, order_size_feature_join_node):
    """Test that join feature node is pruned if the node does not contribute to the final output"""
    project_ts = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["ts"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[order_size_feature_join_node],
    )
    pruned_graph, _ = global_graph.prune(target_node=project_ts, aggressive=True)
    assert pruned_graph.edges_map == {"input_1": ["project_1"]}
    assert pruned_graph.get_node_by_name("project_1") == {
        "name": "project_1",
        "type": "project",
        "output_type": "series",
        "parameters": {"columns": ["ts"]},
    }


def test_join_with_assign_node__join_node_parameters_pruning(
    global_graph, event_data_input_node, item_data_input_node, groupby_node_params
):
    """Test join node parameters pruning"""
    # construct a join node to join an item data & an event data (with a redundant column)
    proj_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["order_id"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[event_data_input_node],
    )
    add_node = global_graph.add_operation(
        node_type=NodeType.ADD,
        node_params={"value": 1},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_node],
    )
    assign_node = global_graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "derived_col"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[event_data_input_node, add_node],
    )
    join_node = global_graph.add_operation(
        node_type=NodeType.JOIN,
        node_params={
            "left_on": "order_id",
            "right_on": "order_id",
            "left_input_columns": ["cust_id", "order_id", "order_method", "derived_col"],
            "left_output_columns": ["cust_id", "order_id", "order_method", "derived_col"],
            "right_input_columns": ["item_type", "item_name"],
            "right_output_columns": ["item_type", "item_name"],
            "join_type": "inner",
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_node, item_data_input_node],
    )

    # perform a groupby on the merged table without using the derived column
    groupby_node_params["parent"] = None
    groupby_node_params["value_by"] = "item_type"
    groupby_node_params["agg_func"] = "count"
    groupby_node_params["names"] = ["item_type_count_30d"]
    groupby_node_params["windows"] = ["30d"]
    groupby_node = add_groupby_operation(
        graph=global_graph,
        groupby_node_params=groupby_node_params,
        input_node=join_node,
    )

    # prune the graph & generate operation structure of the pruned graph
    pruned_graph, node_name_map = global_graph.prune(target_node=groupby_node)
    pruned_graph = QueryGraph(**pruned_graph.json_dict())
    pruned_node = pruned_graph.get_node_by_name(node_name_map[groupby_node.name])

    op_struct = pruned_graph.extract_operation_structure(node=pruned_node)
    assert op_struct.columns == [
        {
            "name": "item_type",
            "node_name": "join_1",
            "node_names": {"join_1", "input_2"},
            "tabular_data_id": None,
            "tabular_data_type": "item_data",
            "type": "source",
            "dtype": "VARCHAR",
            "filter": False,
        }
    ]
    assert op_struct.aggregations == [
        {
            "name": "item_type_count_30d",
            "category": "item_type",
            "column": None,
            "groupby": ["cust_id"],
            "groupby_type": "groupby",
            "method": "count",
            "window": "30d",
            "dtype": "FLOAT",
            "type": "aggregation",
            "node_name": "groupby_1",
            "node_names": {"groupby_1", "input_2", "input_1", "join_1"},
            "filter": False,
        }
    ]

    # check pruned join node
    pruned_join_node = pruned_graph.get_node_by_name("join_1")
    assert pruned_join_node.parameters == {
        "join_type": "inner",
        "left_input_columns": ["cust_id", "order_id", "order_method"],
        "left_on": "order_id",
        "left_output_columns": ["cust_id", "order_id", "order_method"],
        "right_input_columns": ["item_type", "item_name"],
        "right_on": "order_id",
        "right_output_columns": ["item_type", "item_name"],
        "scd_parameters": None,
    }
