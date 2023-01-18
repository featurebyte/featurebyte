"""
Feature validation util
"""
from typing import List

from featurebyte.query_graph.enum import NodeType


def assert_is_lookup_feature(node_types_lineage: List[NodeType]) -> None:
    """
    Checks to see if a feature is a lookup feature.

    Parameters
    ----------
    node_types_lineage: list[NodeType]
        node type lineage

    Raises
    ------
    ValueError
        raised if the feature is not a lookup feature
    """
    if NodeType.LOOKUP in node_types_lineage:
        return
    raise ValueError("feature is not a lookup feature.")
