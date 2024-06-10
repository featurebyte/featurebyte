"""
This module generic query object classes
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, ClassVar, Dict, Optional, Tuple, TypeVar, cast

import json
import operator
from abc import abstractmethod

from cachetools import LRUCache, cachedmethod
from cachetools.keys import hashkey
from pydantic import Field, root_validator
from typeguard import typechecked

from featurebyte.common.utils import get_version
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.algorithm import dfs_traversal
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph, QueryGraph
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.metadata.operation import (
    NodeOutputCategory,
    OperationStructure,
    OperationStructureInfo,
)
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from featurebyte.query_graph.transform.flattening import GraphFlatteningTransformer
from featurebyte.query_graph.transform.sdk_code import SDKCodeExtractor

if TYPE_CHECKING:
    from pydantic.typing import AbstractSetIntStr, MappingIntStrAny


QueryObjectT = TypeVar("QueryObjectT", bound="QueryObject")


def get_operation_structure_cache_key(obj: QueryObjectT) -> Any:
    """
    Returns the cache key QueryObject's OperationStructure cache.

    The cache is used for OperationStructure which is derived from graph, so the hash key is based
    on the graph instance (local or global) and node name.

    Parameters
    ----------
    obj : QueryObjectT
        Instance of the query object

    Returns
    -------
    Any
    """
    graph_identity = 0 if isinstance(obj.graph, GlobalQueryGraph) else id(obj.graph)
    return hashkey(graph_identity, obj.node_name)


def _create_operation_structure_cache() -> Any:
    """
    Create the operation structure cache

    Returns
    -------
    LRUCache
    """
    return LRUCache(maxsize=1024)


class QueryObject(FeatureByteBaseModel):
    """
    QueryObject class contains query graph, node, row index lineage & session.
    """

    # class variables
    _operation_structure_cache: ClassVar[Any] = _create_operation_structure_cache()

    # instance variables
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
    @cachedmethod(
        cache=operator.attrgetter("_operation_structure_cache"),
        key=get_operation_structure_cache_key,
    )
    def operation_structure_info(self) -> OperationStructureInfo:
        """
        Returns the operation structure info of the current node

        Returns
        -------
        OperationStructureInfo
        """
        return self.graph.extract_operation_structure_info(self.node, keep_all_source_columns=True)

    @property
    def operation_structure(self) -> OperationStructure:
        """
        Returns the operation structure of the current node

        Returns
        -------
        OperationStructure
        """
        return self.operation_structure_info.operation_structure_map[self.node_name]

    @property
    def row_index_lineage(self) -> Tuple[str, ...]:
        """
        A list of node names that changes number of rows leading to the current node

        Returns
        -------
        Tuple[str, ...]
        """
        return self.operation_structure.row_index_lineage

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
        flattened_graph, node_name_map = GraphFlatteningTransformer(graph=pruned_graph).transform()
        flattened_node = flattened_graph.get_node_by_name(node_name_map[pruned_node.name])

        # prune the flattened graph as view graph node is not pruned before flattened
        graph = QueryGraph(**flattened_graph.dict())
        pruned_flattened_graph, pruned_node_name_map = graph.prune(flattened_node)
        pruned_flattened_node = pruned_flattened_graph.get_node_by_name(
            pruned_node_name_map[flattened_node.name]
        )

        # construct the node type lineage
        for node in dfs_traversal(pruned_flattened_graph, pruned_flattened_node):
            out.append(node.type)
        return out

    @property
    def output_category(self) -> NodeOutputCategory:
        """
        Returns the output type of the current node (view or feature)

        Returns
        -------
        NodeOutputCategory
        """
        return self.operation_structure.output_category

    @root_validator
    @classmethod
    def _convert_query_graph_to_global_query_graph(cls, values: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(values["graph"], GlobalQueryGraph):
            global_graph, node_name_map = GlobalQueryGraph().load(values["graph"])
            values["graph"] = global_graph
            values["node_name"] = node_name_map[values["node_name"]]
        return values

    def extract_pruned_graph_and_node(self, **kwargs: Any) -> tuple[QueryGraphModel, Node]:
        """
        Extract pruned graph & node from the global query graph

        Parameters
        ----------
        **kwargs: Any
            Additional keyword parameters

        Returns
        -------
        tuple[QueryGraphModel, Node]
            QueryGraph & mapped Node object (within the pruned graph)
        """
        _ = kwargs
        pruned_graph, node_name_map = GlobalQueryGraph().prune(target_node=self.node)
        mapped_node = pruned_graph.get_node_by_name(node_name_map[self.node.name])
        return pruned_graph, mapped_node

    def _generate_code(
        self,
        to_format: bool = False,
        to_use_saved_data: bool = False,
        table_id_to_info: Optional[Dict[PydanticObjectId, Dict[str, Any]]] = None,
    ) -> str:
        """
        Generate SDK codes this graph & node

        Parameters
        ----------
        to_format: bool
            Whether to format generated code (require `black` python package)
        to_use_saved_data: bool
            Whether to use saved table in the SDK codes
        table_id_to_info: Optional[Dict[PydanticObjectId, Dict[str, Any]]]
            Data ID to dictionary info that is not stored in the query graph

        Returns
        -------
        str
        """
        pruned_graph, node = self.extract_pruned_graph_and_node()
        extract_kwargs: dict[str, Any] = {
            "to_use_saved_data": to_use_saved_data,
            "feature_store_name": self.feature_store.name,
            "feature_store_id": self.feature_store.id,
            "database_details": self.feature_store.get_feature_store_details().details,
            "table_id_to_info": table_id_to_info or {},
        }
        state = SDKCodeExtractor(graph=pruned_graph).extract(node=node, **extract_kwargs)
        return state.code_generator.generate(
            to_format=to_format, header_comment=f"# Generated by SDK version: {get_version()}"
        )

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
            pruned_graph, node_name_map = self.graph.quick_prune(target_node_names=[self.node_name])
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

    @classmethod
    def clear_operation_structure_cache(cls) -> None:
        """
        Clear the operation structure cache
        """
        del cls._operation_structure_cache
        cls._operation_structure_cache = _create_operation_structure_cache()

    @typechecked
    def preview_sql(self, limit: int = 10, **kwargs: Any) -> str:
        """
        Generate SQL query to preview the transformation output

        Parameters
        ----------
        limit: int
            maximum number of return rows
        **kwargs: Any
            Additional keyword parameters

        Returns
        -------
        str
        """
        pruned_graph, mapped_node = self.extract_pruned_graph_and_node(**kwargs)
        return GraphInterpreter(
            pruned_graph, source_type=self.feature_store.type
        ).construct_preview_sql(node_name=mapped_node.name, num_rows=limit)[0]


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
