"""
Module for Query Graph pruning related utilities
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from featurebyte.query_graph.graph import QueryGraph

if TYPE_CHECKING:
    from featurebyte.models.feature import FeatureModel
    from featurebyte.query_graph.node import Node


def get_prune_graph_and_nodes(feature_objects: list[FeatureModel]) -> tuple[QueryGraph, list[Node]]:
    """Construct the pruned graph which contains list of pruned feature graph

    Parameters
    ----------
    feature_objects: List[FeatureModel]
        List of FeatureModel objects

    Returns
    -------
    QueryGraph, List[Node]
    """
    local_query_graph = QueryGraph()
    feature_nodes = []
    for feature in feature_objects:
        pruned_graph, mapped_node = feature.extract_pruned_graph_and_node()
        local_query_graph, local_name_map = local_query_graph.load(pruned_graph)
        feature_nodes.append(local_query_graph.get_node_by_name(local_name_map[mapped_node.name]))
    return local_query_graph, feature_nodes
