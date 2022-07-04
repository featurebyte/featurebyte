"""
This module generic query object classes
"""
from __future__ import annotations

from typing import Any, Tuple

from abc import abstractmethod

import pandas as pd
from pydantic import BaseModel, Field, root_validator

from featurebyte.config import Configurations, Credentials
from featurebyte.models.feature_store import FeatureStoreModel, TableDetails
from featurebyte.query_graph.graph import GlobalQueryGraph, Node, QueryGraph
from featurebyte.query_graph.interpreter import GraphInterpreter
from featurebyte.session.base import BaseSession
from featurebyte.session.manager import SessionManager


class ExtendedFeatureStoreModel(FeatureStoreModel):
    """
    ExtendedFeatureStoreModel class contains method to construct a session
    """

    def get_session(self, credentials: Credentials | None = None) -> BaseSession:
        """
        Get data source session based on provided configuration

        Parameters
        ----------
        credentials: Credentials
            data source to credential mapping used to initiate a new connection

        Returns
        -------
        BaseSession
        """
        if credentials is None:
            config = Configurations()
            credentials = config.credentials
        session_manager = SessionManager(credentials=credentials)
        return session_manager[self]


class QueryObject(BaseModel):
    """
    QueryObject class contains query graph, node, row index lineage & session.
    """

    graph: QueryGraph = Field(default_factory=GlobalQueryGraph)
    node: Node
    row_index_lineage: Tuple[str, ...]
    tabular_source: Tuple[FeatureStoreModel, TableDetails]

    def __repr__(self) -> str:
        return f"{type(self).__name__}(node.name={self.node.name})"

    def __str__(self) -> str:
        return repr(self)

    @root_validator()
    @classmethod
    def _convert_query_graph_to_global_query_graph(cls, values: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(values["graph"], GlobalQueryGraph):
            global_graph, node_name_map = GlobalQueryGraph().load(values["graph"])
            values["graph"] = global_graph
            values["node"] = global_graph.get_node_by_name(node_name_map[values["node"].name])
        return values

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
        if isinstance(self.graph, GlobalQueryGraph):
            pruned_graph, mapped_node = self.graph.prune(
                target_node=self.node, target_columns=set(columns)
            )
            return GraphInterpreter(pruned_graph).construct_preview_sql(
                node_name=mapped_node.name, num_rows=limit
            )
        return GraphInterpreter(self.graph).construct_preview_sql(
            node_name=self.node.name, num_rows=limit
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
        str
        """
        return self._preview_sql(columns=[], limit=limit)

    def preview(self, limit: int = 10, credentials: Credentials | None = None) -> pd.DataFrame:
        """
        Preview transformed table/column partial output

        Parameters
        ----------
        limit: int
            maximum number of return rows
        credentials: Credentials | None
            credentials to create a database session

        Returns
        -------
        pd.DataFrame
        """
        session = self.get_session(credentials)
        return session.execute_query(self.preview_sql(limit=limit))

    def get_session(self, credentials: Credentials | None = None) -> BaseSession:
        """
        Get a session based on underlying tabular source and provided credentials

        Parameters
        ----------
        credentials : Credentials
            data source to credential mapping used to initiate a new connection

        Returns
        -------
        BaseSession
        """
        if credentials is None:
            config = Configurations()
            credentials = config.credentials

        data_source = ExtendedFeatureStoreModel(**self.tabular_source[0].dict())
        session = data_source.get_session(credentials=credentials)
        return session

    def _to_dict(self, target_columns: set[str], *args: Any, **kwargs: Any) -> dict[str, Any]:
        if isinstance(self.graph, GlobalQueryGraph):
            pruned_graph, mapped_node = self.graph.prune(
                target_node=self.node, target_columns=target_columns
            )
            new_object = self.copy()
            new_object.graph = pruned_graph
            new_object.node = mapped_node
            return new_object.dict(**kwargs)
        return super().dict(*args, **kwargs)


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
    @abstractmethod
    def inherited_columns(self) -> set[str]:
        """
        Special columns set which will be automatically added to the object of same class
        derived from current object

        Returns
        -------
        set[str]
        """

    @property
    def inception_node(self) -> Node:
        """
        Node where the event source is introduced to the query graph

        Returns
        -------
        Node
        """
        graph = GlobalQueryGraph()
        return graph.get_node_by_name(self.row_index_lineage[0])
