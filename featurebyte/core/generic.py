"""
This module generic query object classes
"""
from __future__ import annotations

from typing import Any, Tuple

from abc import abstractmethod

import pandas as pd
from pydantic import BaseModel, Field

from featurebyte.query_graph.graph import GlobalQueryGraph, Node
from featurebyte.query_graph.interpreter import GraphInterpreter


class QueryObject(BaseModel):
    """
    QueryObject class contains query graph, node, row index lineage & session.
    """

    graph: GlobalQueryGraph = Field(default_factory=GlobalQueryGraph)
    node: Node
    row_index_lineage: Tuple[str, ...]
    session: Any

    def __repr__(self) -> str:
        return f"{type(self).__name__}(node.name={self.node.name})"

    def __str__(self) -> str:
        return repr(self)

    def _preview_sql(self, columns: list[str], limit: int = 10) -> str:
        """
        Preview SQL query

        Parameters
        ----------
        columns: list[str]
            list of columns used to prune the query graph
        limit: int
            maximum number of return rows

        Returns
        -------
        str
        """
        pruned_graph, mapped_node = self.graph.prune(
            target_node=self.node, target_columns=set(columns)
        )
        return GraphInterpreter(pruned_graph).construct_preview_sql(
            mapped_node.name, num_rows=limit
        )

    def preview_sql(self, limit: int = 10) -> str:
        """
        Generate SQL query to preview the transformation output

        Parameters
        ----------
        limit: int
            maximum number of return rows

        Returns
        -------
        pd.DataFrame | None
        """
        return self._preview_sql(columns=[], limit=limit)

    def preview(self, limit: int = 10) -> pd.DataFrame | None:
        """
        Preview transformed table/column partial output

        Parameters
        ----------
        limit: int
            maximum number of return rows

        Returns
        -------
        pd.DataFrame | None
        """
        if self.session:
            return self.session.execute_query(self.preview_sql(limit=limit))
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

            if isinstance(attr_val, str):
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
        graph = GlobalQueryGraph()
        return graph.get_node_by_name(self.row_index_lineage[0])
