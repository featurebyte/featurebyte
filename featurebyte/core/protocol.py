"""
This module contains all protocol related classes
"""
# pylint: disable=R0903
from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from featurebyte.query_graph.graph import Node, QueryGraph
    from featurebyte.session.base import BaseSession


class HasRowIndexLineageProtocol(Protocol):
    """
    Class with row_index_lineage property/attribute
    """

    @property
    def row_index_lineage(self) -> tuple[str, ...]:
        """
        Attribute/property to indicate row index lineage

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
        Attribute/property to indicate the first node in row_index_lineage

        Returns
        -------
        tuple[str, ...]
        """


class PreviewableProtocol(Protocol):
    """
    Class with query graph and session attributes/properties
    """

    @property
    def graph(self) -> QueryGraph:
        """
        Query graph object

        Returns
        -------
        QueryGraph
        """

    @property
    def node(self) -> Node:
        """
        Current operation node

        Returns
        -------
        Node
        """

    @property
    def session(self) -> BaseSession:
        """
        Session to interface with database

        Returns
        -------
        BaseSession
        """
