"""
Test flattening
"""

from featurebyte.query_graph.enum import GraphNodeType
from featurebyte.query_graph.node.nested import BaseGraphNode
from featurebyte.query_graph.transform.flattening import GraphFlatteningTransformer


def test_flatten_graph__flatten_cleaning_node(feature_with_cleaning_operations):
    """
    Test flatten graph without skip cleaning parameters
    """
    pruned_graph, _ = feature_with_cleaning_operations.extract_pruned_graph_and_node()
    # Verify the original state of the graph by checking that there is a cleaning graph node
    original_node_names = list(pruned_graph.nodes_map.keys())

    assert original_node_names == ["input_1", "graph_1", "groupby_1", "project_1"]
    graph_node = pruned_graph.get_node_by_name("graph_1")
    inner_graph = graph_node.parameters.graph
    graph_node_node_names = list(inner_graph.nodes_map.keys())
    # Verify that the `graph_1` node here is the cleaning node
    assert graph_node_node_names == ["proxy_input_1", "project_1", "graph_1"]
    cleaning_graph_node = inner_graph.get_node_by_name("graph_1")
    assert cleaning_graph_node.parameters.type == GraphNodeType.CLEANING
    cleaning_graph_node_node_names = list(cleaning_graph_node.parameters.graph.nodes_map.keys())
    assert cleaning_graph_node_node_names == [
        "proxy_input_1",
        "project_1",
        "is_null_1",
        "conditional_1",
        "cast_1",
        "assign_1",
    ]

    # Try to flatten the graph
    transformer = GraphFlatteningTransformer(pruned_graph)
    flattened_graph, _ = transformer.transform()

    # Verify that the flattened graph looks correct.
    # We don't expect any more graph nodes in here.
    new_graph_node_names = list(flattened_graph.nodes_map.keys())
    assert set(new_graph_node_names) == {
        "input_1",
        "project_1",
        "project_2",
        "is_null_1",
        "conditional_1",
        "cast_1",
        "assign_1",
        "groupby_1",
        "project_3",
    }


def test_flatten_graph__dont_flatten_cleaning_node(feature_with_cleaning_operations):
    """
    Test flatten graph with skip flattening cleaning node parameters
    """
    pruned_graph, _ = feature_with_cleaning_operations.extract_pruned_graph_and_node()
    # Verify the original state of the graph
    original_node_names = list(pruned_graph.nodes_map.keys())
    assert original_node_names == ["input_1", "graph_1", "groupby_1", "project_1"]
    graph_node = pruned_graph.get_node_by_name("graph_1")
    inner_graph = graph_node.parameters.graph
    graph_node_node_names = list(inner_graph.nodes_map.keys())
    # Verify that the `graph_1` node here is the cleaning node
    assert graph_node_node_names == ["proxy_input_1", "project_1", "graph_1"]
    cleaning_graph_node = inner_graph.get_node_by_name("graph_1")
    assert cleaning_graph_node.parameters.type == GraphNodeType.CLEANING

    # Try to flatten the graph
    transformer = GraphFlatteningTransformer(pruned_graph)
    flattened_graph, _ = transformer.transform(
        skip_flattening_graph_node_types={GraphNodeType.CLEANING}
    )

    # Verify that the flattened graph looks correct
    new_graph_node_names = list(flattened_graph.nodes_map.keys())
    assert set(new_graph_node_names) == {
        "input_1",
        "graph_1",
        "project_1",
        "groupby_1",
        "project_2",
    }
    cleaning_graph_node = flattened_graph.get_node_by_name("graph_1")
    assert isinstance(cleaning_graph_node, BaseGraphNode)
    assert cleaning_graph_node.parameters.type == GraphNodeType.CLEANING
