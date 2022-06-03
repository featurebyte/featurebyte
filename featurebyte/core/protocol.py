"""
This module contains all protocol related classes
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from featurebyte.query_graph.graph import Node, QueryGraph
    from featurebyte.session.base import BaseSession


class WithQueryGraphProtocol(Protocol):
    """
    Class contains query graph related attributes
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
        Node represent the current Frame/Series output

        Returns
        -------
        Node
        """

    @property
    def row_index_lineage(self) -> tuple[str, ...]:
        """
        Tuple which stores the row index operation lineage

        Returns
        -------
        tuple[str, ...]
        """


class ProtectedPropertiesProtocol(WithQueryGraphProtocol):
    """
    Class with inception_node property/attribute
    """

    @property
    def protected_attributes(self) -> list[str]:
        """
        List of protected attributes used to extract protected_columns

        Returns
        -------
        list[str]
        """

    @property
    def protected_columns(self) -> set[str]:
        """
        Special columns set where values of these columns should not be overridden

        Returns
        -------
        set[str]
        """

    @property
    def inception_node(self) -> Node:
        """
        Attribute/property to indicate the first node in row_index_lineage

        Returns
        -------
        tuple[str, ...]
        """


class WithQueryGraphAndSessionProtocol(WithQueryGraphProtocol):
    """
    Class with query graph and session attributes/properties
    """

    session: BaseSession
