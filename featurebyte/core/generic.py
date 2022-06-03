"""
This module generic query object classes
"""
from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass

import pandas as pd

from featurebyte.query_graph.graph import Node, QueryGraph
from featurebyte.query_graph.interpreter import GraphInterpreter
from featurebyte.session.base import BaseSession


@dataclass
class QueryObject:
    """
    QueryObject class contains query graph, node, row index lineage & session.
    """

    graph: QueryGraph
    node: Node
    row_index_lineage: tuple[str, ...]
    session: BaseSession | None

    def __repr__(self) -> str:
        return f"{type(self).__name__}(node.name={self.node.name})"

    def preview(self) -> pd.DataFrame | None:
        """
        Preview transformed table/column partial output

        Returns
        -------
        pd.DataFrame | None
        """
        sql_query = GraphInterpreter(self.graph).construct_preview_sql(self.node.name)
        if self.session:
            return self.session.execute_query(sql_query)
        return None


class ProtectedColumnsQueryObject(QueryObject):
    """
    QueryObject contains at least one or more protected column(s). The protected column should not be overridden
    or remove from the parent node.
    """

    @property
    @abstractmethod
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

        Raises
        ------
        TypeError
            if any of the protected attribute types is not expected
        """
        columns = []
        for attr in self.protected_attributes:
            attr_val = getattr(self, attr)
            if attr_val is None:
                continue
            elif isinstance(attr_val, str):
                columns.append(attr_val)
            elif isinstance(attr_val, list) and all(isinstance(elem, str) for elem in attr_val):
                columns.extend(attr_val)
            else:
                raise TypeError(f"Unsupported type for protected attribute '{attr}'!")
        return set(columns)

    @property
    def inception_node(self) -> Node:
        """
        Input node where the event source is introduced to the query graph

        Returns
        -------
        Node
        """
        graph = QueryGraph()
        return graph.get_node_by_name(self.row_index_lineage[0])
