"""
This module contains all protocol related classes
"""
# pylint: disable=R0903
from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from featurebyte.query_graph.graph import Node


class HasRowIndexLineageProtocol(Protocol):
    """
    Class with row_index_lineage property/attribute
    """

    @property
    def row_index_lineage(self) -> tuple[str, ...]:
        """
        Attributes/property to indicate row index lineage

        Returns
        -------
        tuple[str, ...]
        """


class HasInceptionNodeProtocol(Protocol):
    """
    Class with inception_node property/attribute
    """

    @property
    def inception_node(self) -> Node:
        """
        Attributes/property to indicate the first node in row_index_lineage

        Returns
        -------
        tuple[str, ...]
        """
