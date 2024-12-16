"""
This module generic query object classes
"""

from __future__ import annotations

import operator
from abc import abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Optional, Tuple, TypeVar

from cachetools import LRUCache, cachedmethod
from cachetools.keys import hashkey
from pydantic import Field, model_validator
from typeguard import typechecked

from featurebyte.common.utils import get_version
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.algorithm import dfs_traversal
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph, QueryGraph
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema, TimeZoneColumn
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
    tabular_source: TabularSource = Field(frozen=True)
    feature_store: FeatureStoreModel = Field(exclude=True, frozen=True)

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
        graph = QueryGraph(**flattened_graph.model_dump(by_alias=True))
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

    @model_validator(mode="after")
    def _convert_query_graph_to_global_query_graph(self) -> "QueryObject":
        if not isinstance(self.graph, GlobalQueryGraph):
            global_graph, node_name_map = GlobalQueryGraph().load(self.graph)
            # assign to __dict__ to avoid infinite recursion due to model_validator(mode="after") call with
            # validate_assign=True in model_config.
            self.__dict__["graph"] = global_graph
            self.__dict__["node_name"] = node_name_map[self.node_name]
        return self

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
        update_dict.update({"feature_store": self.feature_store.model_copy(deep=deep)})
        return self.model_copy(update=update_dict, deep=deep)

    def model_dump(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        if isinstance(self.graph, GlobalQueryGraph):
            pruned_graph, node_name_map = self.graph.quick_prune(target_node_names=[self.node_name])
            mapped_node = pruned_graph.get_node_by_name(node_name_map[self.node.name])
            new_object = self.copy()
            new_object.node_name = mapped_node.name

            # Use the __dict__ assignment method to skip pydantic validation check. Otherwise, it will trigger
            # `_convert_query_graph_to_global_query_graph` validation check and convert the pruned graph into
            # global one.
            new_object.__dict__["graph"] = pruned_graph
            return new_object.model_dump(*args, **kwargs)
        return dict(super().model_dump(*args, **kwargs))

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
            pruned_graph, source_info=self.feature_store.get_source_info()
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
            elif isinstance(attr_val, TimestampSchema):
                if attr_val.timezone and isinstance(attr_val.timezone, TimeZoneColumn):
                    columns.extend(attr_val.timezone.column_name)
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
