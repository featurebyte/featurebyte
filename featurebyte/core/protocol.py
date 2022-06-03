"""
This module contains all protocol related classes
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from featurebyte.query_graph.graph import Node, QueryGraph


class WithQueryGraphProtocol(Protocol):
    """
    Class contains query graph related attributes
    """

    graph: QueryGraph
    node: Node
    row_index_lineage: tuple[str, ...]
