"""
This module generic query object classes
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Tuple, TypeVar, cast

import json
from abc import abstractmethod

from pydantic import Field, root_validator
from typeguard import typechecked

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.algorithm import dfs_traversal
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph, QueryGraph
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.interpreter import GraphInterpreter

if TYPE_CHECKING:
    from pydantic.typing import AbstractSetIntStr, MappingIntStrAny


QueryObjectT = TypeVar("QueryObjectT", bound="QueryObject")


class QueryObject(FeatureByteBaseModel):
    """
    QueryObject class contains query graph, node, row index lineage & session.
    """

    graph: QueryGraph = Field(default_factory=GlobalQueryGraph)
    node_name: str
    tabular_source: TabularSource = Field(allow_mutation=False)
    feature_store: FeatureStoreModel = Field(exclude=True, allow_mutation=False)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(node_name={self.node_name})"

    def __str__(self) -> str:
        return repr(self)

    @property
    def node(self) -> Node:
        """
        Representative of the current object in the graph

        Returns
        -------
        Node
        """
        return self.graph.get_node_by_name(self.node_name)

    @property
    def row_index_lineage(self) -> Tuple[str, ...]:
        """
        A list of node names that changes number of rows leading to the current node

        Returns
        -------
        Tuple[str, ...]
        """
        operation_structure = self.graph.extract_operation_structure(self.node)
        return operation_structure.row_index_lineage

    @property
    def node_types_lineage(self) -> list[NodeType]:
        """
        Returns a list of node types that is part of the lineage of this QueryObject

        Returns
        -------
        list[NodeType]
        """
        out = []
        pruned_graph, pruned_node = self.extract_pruned_graph_and_node()
        for node in dfs_traversal(pruned_graph, pruned_node):
            out.append(node.type)
        return out

    @root_validator()
    @classmethod
    def _convert_query_graph_to_global_query_graph(cls, values: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(values["graph"], GlobalQueryGraph):
            global_graph, node_name_map = GlobalQueryGraph().load(values["graph"])
            values["graph"] = global_graph
            values["node_name"] = node_name_map[values["node_name"]]
        return values

    def extract_pruned_graph_and_node(self) -> tuple[QueryGraphModel, Node]:
        """
        Extract pruned graph & node from the global query graph

        Parameters
        ----------
        Returns
        -------
        tuple[QueryGraphModel, Node]
            QueryGraph & mapped Node object (within the pruned graph)
        """
        pruned_graph, node_name_map = GlobalQueryGraph().prune(
            target_node=self.node, aggressive=True
        )
        mapped_node = pruned_graph.get_node_by_name(node_name_map[self.node.name])
        return pruned_graph, mapped_node

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
        return GraphInterpreter(
            pruned_graph, source_type=self.feature_store.type
        ).construct_preview_sql(node_name=mapped_node.name, num_rows=limit)[0]

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

    def dict(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        if isinstance(self.graph, GlobalQueryGraph):
            pruned_graph, node_name_map = self.graph.prune(target_node=self.node, aggressive=False)
            mapped_node = pruned_graph.get_node_by_name(node_name_map[self.node.name])
            new_object = self.copy()
            new_object.node_name = mapped_node.name

            # Use the __dict__ assignment method to skip pydantic validation check. Otherwise, it will trigger
            # `_convert_query_graph_to_global_query_graph` validation check and convert the pruned graph into
            # global one.
            new_object.__dict__["graph"] = pruned_graph
            return new_object.dict(*args, **kwargs)
        return super().dict(*args, **kwargs)

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
            for key in ["graph", "node_name"]:
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
    def inherited_columns(self) -> set[str]:
        """
        Special columns set which will be automatically added to the object of same class
        derived from current object

        Returns
        -------
        set[str]
        """
        return set()
