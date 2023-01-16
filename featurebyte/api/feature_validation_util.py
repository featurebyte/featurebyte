"""
Feature validation util
"""
from featurebyte.query_graph.enum import NodeType


def is_lookup_feature(node_types_lineage: list[NodeType]) -> None:
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
    raise ValueError(f"feature is not a lookup feature.")
