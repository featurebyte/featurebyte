"""
Generic graph related algorithms
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Iterator, List

if TYPE_CHECKING:
    from featurebyte.query_graph.model.graph import QueryGraphModel
    from featurebyte.query_graph.node import Node


def dfs_traversal(query_graph: QueryGraphModel, node: Node) -> Iterator[Node]:
    """Perform a DFS traversal

    Parameters
    ----------
    query_graph : QueryGraphModel
        Query graph
    node : Node
        Current node to traverse from

    Yields
    ------
    Node
        Query graph nodes
    """
    yield from dfs_inner(query_graph, node, {})


def dfs_inner(query_graph: QueryGraphModel, node: Node, visited: dict[str, bool]) -> Iterator[Node]:
    """Performs the actual work of a DFS traversal

    Parameters
    ----------
    query_graph : QueryGraphModel
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
    for parent_name in query_graph.backward_edges_map[node.name]:
        if visited.get(parent_name):
            continue
        parent_node = query_graph.get_node_by_name(parent_name)
        yield from dfs_inner(query_graph, parent_node, visited)


def _topological_sort_util(
    edges_map: Dict[str, Any], node_name: str, visited: dict[str, bool], stack: list[str]
) -> None:
    # mark node as visited
    visited[node_name] = True

    # recur for all the vertices adjacent to this vertex
    for adj_node_name in edges_map.get(node_name, []):
        if not visited[adj_node_name]:
            _topological_sort_util(edges_map, adj_node_name, visited, stack)

    stack.append(node_name)


def topological_sort(node_names: List[str], edges_map: Dict[str, Any]) -> list[str]:
    """
    Topological sort the graph (reference: https://www.geeksforgeeks.org/topological-sorting/)

    Parameters
    ----------
    node_names: List[str]
        List of node names
    edges_map: Dict[str, Any]
        Adjacency list of the graph

    Returns
    -------
    list[str]
        List of node names in topological sorted order
    """
    # construct list of connected node names
    visited = {node_name: False for node_name in node_names}
    stack: list[str] = []
    for node_name in node_names:
        if not visited[node_name]:
            _topological_sort_util(edges_map, node_name, visited, stack)

    return stack[::-1]
