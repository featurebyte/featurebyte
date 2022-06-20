"""
Generic graph related algorithms
"""
from __future__ import annotations

from typing import Iterator

from featurebyte.query_graph.graph import Node, QueryGraph


def dfs_traversal(query_graph: QueryGraph, node: Node) -> Iterator[Node]:
    """Perform a DFS traversal

    Parameters
    ----------
    query_graph : QueryGraph
        Query graph
    node : Node
        Current node to traverse from

    Yields
    ------
    Node
        Query graph nodes
    """
    yield from dfs_inner(query_graph, node, {})


def dfs_inner(query_graph: QueryGraph, node: Node, visited: dict[str, bool]) -> Iterator[Node]:
    """Performs the actual work of a DFS traversal

    Parameters
    ----------
    query_graph : QueryGraph
        Query graph
    node : Node
        Current node to traverse from
    visited : dict
        Markers for visited nodes

    Yields
    ------
    Node
        Query graph nodes
    """
    visited[node.name] = True
    yield node
    for parent_name in query_graph.backward_edges[node.name]:
        if visited.get(parent_name):
            continue
        parent_node = query_graph.get_node_by_name(parent_name)
        yield from dfs_inner(query_graph, parent_node, visited)
