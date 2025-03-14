"""
Utility functions for checking and handling TTLs in the graph.
"""

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import BaseWindowAggregateParameters


def is_ttl_handling_required(node: Node) -> bool:
    """
    Check if TTL handling is required for the given node

    Parameters
    ----------
    node: Node
        Node for which TTL handling is to be checked

    Returns
    -------
    bool
    """
    if node.type in {NodeType.GROUPBY, NodeType.NON_TILE_WINDOW_AGGREGATE}:
        assert isinstance(node.parameters, BaseWindowAggregateParameters)
        if any(node.parameters.windows):
            # if the window has any value that is not None, it requires TTL handling
            # None value means the window is not limited (thus no TTL handling is required)
            return True

    if node.type in {NodeType.TIME_SERIES_WINDOW_AGGREGATE}:
        # time series window aggregate requires TTL handling as it has a window
        return True
    return False
