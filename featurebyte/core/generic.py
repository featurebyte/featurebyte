"""
This module generic query object classes
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Tuple, TypeVar, cast

import json
from abc import abstractmethod
from http import HTTPStatus

import pandas as pd
from pydantic import Field, StrictStr
from typeguard import typechecked

from featurebyte.config import Configurations, Credentials
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature_store import FeatureStoreModel, TabularSource
from featurebyte.query_graph.graph import GlobalQueryGraph, QueryGraph
from featurebyte.query_graph.interpreter import GraphInterpreter
from featurebyte.query_graph.node import Node
from featurebyte.schema.feature_store import FeatureStorePreview
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


QueryObjectT = TypeVar("QueryObjectT", bound="QueryObject")


class QueryObject(FeatureByteBaseModel):
    """
    QueryObject class contains query graph, node, row index lineage & session.
    """

    graph: QueryGraph = Field(default_factory=GlobalQueryGraph)
    node_name: str
    row_index_lineage: Tuple[StrictStr, ...]
    tabular_source: TabularSource = Field(allow_mutation=False)
    feature_store: ExtendedFeatureStoreModel = Field(exclude=True, allow_mutation=False)

    @property
    def node(self) -> Node:
        """
        Representative of the current object in the graph

        Returns
        -------
        Node
        """
        return self.graph.get_node_by_name(self.node_name)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(node_name={self.node_name})"

    def __str__(self) -> str:
        return repr(self)

    @abstractmethod
    def extract_pruned_graph_and_node(self) -> tuple[QueryGraph, Node]:
        """
        Extract pruned graph & node from the global query graph

        Returns
        -------
        QueryGraph & mapped Node object (within the pruned graph)
        """

    @typechecked
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
        pruned_graph, mapped_node = self.extract_pruned_graph_and_node()
        return GraphInterpreter(pruned_graph).construct_preview_sql(
            node_name=mapped_node.name, num_rows=limit
        )

    @typechecked
    def preview(self, limit: int = 10) -> pd.DataFrame:
        """
        Preview transformed table/column partial output

        Parameters
        ----------
        limit: int
            maximum number of return rows

        Returns
        -------
        pd.DataFrame

        Raises
        ------
        RecordRetrievalException
            Preview request failed
        """
        if self.feature_store.details.is_local_source:
            session = self.feature_store.get_session()
            return session.execute_query(self.preview_sql(limit=limit))

        pruned_graph, mapped_node = self.extract_pruned_graph_and_node()
        payload = FeatureStorePreview(
            feature_store_name=self.feature_store.name,
            graph=pruned_graph,
            node=mapped_node,
        )
        client = Configurations().get_client()
        response = client.post(
            url=f"/feature_store/preview?limit={limit}", json=payload.json_dict()
        )
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)
        return pd.read_json(response.json(), orient="table", convert_dates=False)

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

        session = self.feature_store.get_session(credentials=credentials)
        return session

    def copy(
        self: QueryObjectT,
        *,
        include: AbstractSetIntStr | MappingIntStrAny | None = None,
        exclude: AbstractSetIntStr | MappingIntStrAny | None = None,
        update: dict[str, Any] | None = None,
        deep: bool = False,
    ) -> QueryObjectT:
        update_dict = update or {}
        update_dict.update({"feature_store": self.feature_store.copy(deep=deep)})
        return super().copy(
            include=include,
            exclude=exclude,
            update=update_dict,
            deep=deep,
        )

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
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            skip_defaults=skip_defaults,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            encoder=encoder,
            models_as_dict=models_as_dict,
            **dumps_kwargs,
        )
        encoder = cast(Callable[[Any], Any], encoder or self.__json_encoder__)
        dict_object = json.loads(json_object)
        if "graph" in dict_object:
            pruned_dict_object = self.dict()
            for key in ["graph", "node_name", "column_lineage_map", "row_index_lineage"]:
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
