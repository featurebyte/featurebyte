"""
Unit tests for featurebyte.query_graph.algorithms
"""
from featurebyte.query_graph.algorithm import dfs_traversal, topological_sort
from featurebyte.query_graph.enum import NodeOutputType, NodeType


def test_dfs__1(query_graph_with_groupby):
    """Test case for DFS traversal"""
    node = query_graph_with_groupby.get_node_by_name("input_1")
    result = list(dfs_traversal(query_graph_with_groupby, node))
    traverse_sequence = [x.name for x in result]
    assert traverse_sequence == ["input_1"]


def test_dfs__2(query_graph_with_groupby):
    """Test case for DFS traversal"""
    node = query_graph_with_groupby.get_node_by_name("project_2")
    result = list(dfs_traversal(query_graph_with_groupby, node))
    traverse_sequence = [x.name for x in result]
    assert traverse_sequence == ["project_2", "input_1"]


def test_dfs__3(query_graph_with_groupby):
    """Test case for DFS traversal"""
    node = query_graph_with_groupby.get_node_by_name("groupby_1")
    result = list(dfs_traversal(query_graph_with_groupby, node))
    traverse_sequence = [x.name for x in result]
    assert traverse_sequence == [
        "groupby_1",
        "assign_1",
        "input_1",
        "add_1",
        "project_1",
        "project_2",
    ]


def test_dfs__skip_node_type(query_graph_with_groupby):
    """Test case for DFS traversal with skip node type"""
    node = query_graph_with_groupby.get_node_by_name("groupby_1")
    node = query_graph_with_groupby.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["some_col"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node],
    )
    result = list(dfs_traversal(query_graph_with_groupby, node, skip_node_type=NodeType.GROUPBY))
    traverse_sequence = [x.name for x in result]
    assert traverse_sequence == ["project_3"]


def test_topological_sort__0():
    """
    Test topological sort on empty graph edge case
    """
    assert not topological_sort([], {})


def test_topological_sort__1(graph_single_node):
    """
    Test topological sort on single node
    """
    graph, _ = graph_single_node
    assert topological_sort(list(graph.nodes_map), graph.edges_map) == ["input_1"]


def test_topological_sort__2(graph_two_nodes):
    """
    Test topological sort on two nodes
    """
    graph, _, _ = graph_two_nodes
    assert topological_sort(list(graph.nodes_map), graph.edges_map) == ["input_1", "project_1"]


def test_topological_sort__3(graph_three_nodes):
    """
    Test topological sort on three nodes
    """
    graph, _, _, _ = graph_three_nodes
    assert topological_sort(list(graph.nodes_map), graph.edges_map) == [
        "input_1",
        "project_1",
        "eq_1",
    ]


def test_topological_sort__4(graph_four_nodes):
    """
    Test topological sort on four nodes
    """
    graph, _, _, _, _ = graph_four_nodes
    assert topological_sort(list(graph.nodes_map), graph.edges_map) == [
        "input_1",
        "project_1",
        "eq_1",
        "filter_1",
    ]


def test_topological_sort__5(query_graph_with_groupby):
    """
    Test topological sort on query graph with groupby
    """
    node_names = list(query_graph_with_groupby.nodes_map)
    assert topological_sort(node_names, query_graph_with_groupby.edges_map) == [
        "input_1",
        "project_2",
        "project_1",
        "add_1",
        "assign_1",
        "groupby_1",
    ]
