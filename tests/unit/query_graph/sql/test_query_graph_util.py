"""
Test query graph util module
"""
from featurebyte.enum import DBVarType
from featurebyte.query_graph.sql.query_graph_util import get_parent_dtype


def test_get_parent_dtype(query_graph_and_assign_node):
    """
    Test get_parent_dtype
    """
    graph, node = query_graph_and_assign_node
    dtype = get_parent_dtype("c", graph, node)
    assert dtype == DBVarType.FLOAT
