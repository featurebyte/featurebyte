"""
This module generic query object classes
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Tuple

import json
from abc import abstractmethod

import pandas as pd
from pydantic import Field, StrictStr

from featurebyte.config import Configurations, Credentials
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature_store import FeatureStoreModel, TableDetails
from featurebyte.query_graph.graph import GlobalQueryGraph, Node, QueryGraph
from featurebyte.query_graph.interpreter import GraphInterpreter
from featurebyte.session.base import BaseSession
from featurebyte.session.manager import SessionManager

if TYPE_CHECKING:
    from pydantic.typing import AbstractSetIntStr, MappingIntStrAny


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


class QueryObject(FeatureByteBaseModel):
    """
    QueryObject class contains query graph, node, row index lineage & session.
    """

    graph: QueryGraph = Field(default_factory=GlobalQueryGraph)
    node: Node
    row_index_lineage: Tuple[StrictStr, ...]
    tabular_source: Tuple[FeatureStoreModel, TableDetails]

    class Config:
        """
        QueryObject configuration
        """

        copy_on_model_validation = False

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
        if isinstance(self.graph, GlobalQueryGraph):
            pruned_graph, node_name_map = self.graph.prune(
                target_node=self.node, target_columns=set(columns)
            )
            mapped_node = pruned_graph.get_node_by_name(node_name_map[self.node.name])
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

    def json(
        self,
        *,
        include: AbstractSetIntStr | MappingIntStrAny | None = None,
        exclude: AbstractSetIntStr | MappingIntStrAny | None = None,
        by_alias: bool = False,
        skip_defaults: bool | None = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        encoder: Callable[[Any], Any] | None = None,
        models_as_dict: bool = True,
        **dumps_kwargs: Any,
    ) -> str:
        # Serialization of query object requires both graph & node (to prune the graph).
        # However, pydantic `json()` does not call `dict()` directly. It iterates inner attributes
        # and trigger theirs `dict()`. To fix this issue, we call the pydantic `json()` first to
        # serialize the whole object, then calling `QueryObject.dict()` to construct pruned graph & node map.
        # After that, use the `QueryObject.dict()` result to overwrite pydantic `json()` results.
        json_object = super().json(
            include=include,  # type: ignore
            exclude=exclude,  # type: ignore
            by_alias=by_alias,
            skip_defaults=skip_defaults,  # type: ignore
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            encoder=encoder,
            models_as_dict=models_as_dict,
            **dumps_kwargs,
        )
        dict_object = json.loads(json_object)
        if "graph" in dict_object:
            pruned_dict_object = self.dict()
            for key in ["graph", "node", "lineage", "column_lineage_map", "row_index_lineage"]:
                if key in dict_object:
                    dict_object[key] = pruned_dict_object[key]
            json_object = self.__config__.json_dumps(dict_object, default=encoder, **dumps_kwargs)
        return json_object


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
        Node where the event data is introduced to the query graph

        Returns
        -------
        Node
        """
        graph = GlobalQueryGraph()
        return graph.get_node_by_name(self.row_index_lineage[0])
