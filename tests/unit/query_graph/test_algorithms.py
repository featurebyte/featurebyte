"""
Unit tests for featurebyte.query_graph.algorithms
"""
from featurebyte.query_graph.algorithms import dfs_traversal


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
