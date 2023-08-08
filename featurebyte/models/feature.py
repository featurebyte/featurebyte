"""
This module contains Feature related models
"""
from __future__ import annotations

from typing import Any, List, Optional

import pymongo
from bson.objectid import ObjectId
from pydantic import Field, PrivateAttr, root_validator, validator

from featurebyte.common.validator import version_validator
from featurebyte.enum import DBVarType
from featurebyte.models.base import (
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
    VersionIdentifier,
)
from featurebyte.models.feature_namespace import FeatureReadiness
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.metadata.operation import GroupOperationStructure
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from featurebyte.query_graph.sql.online_store_compute_query import (
    get_online_store_precompute_queries,
)


class BaseFeatureModel(FeatureByteCatalogBaseDocumentModel):
    """
    BaseFeatureModel is the base class for FeatureModel & TargetModel.
    It contains all the attributes that are shared between FeatureModel & TargetModel.
    """

    dtype: DBVarType = Field(allow_mutation=False, default=DBVarType.UNKNOWN)
    node_name: str
    tabular_source: TabularSource = Field(allow_mutation=False)
    version: VersionIdentifier = Field(allow_mutation=False, default=None)
    definition: Optional[str] = Field(allow_mutation=False, default=None)

    # special handling for those attributes that are expensive to deserialize
    # internal_* is used to store the raw data from persistence, _* is used as a cache
    internal_graph: Any = Field(allow_mutation=False, alias="graph")
    _graph: Optional[QueryGraph] = PrivateAttr(default=None)

    # list of IDs attached to this feature or target
    entity_ids: List[PydanticObjectId] = Field(allow_mutation=False, default_factory=list)
    table_ids: List[PydanticObjectId] = Field(allow_mutation=False, default_factory=list)
    primary_table_ids: List[PydanticObjectId] = Field(allow_mutation=False, default_factory=list)
    user_defined_function_ids: List[PydanticObjectId] = Field(
        allow_mutation=False, default_factory=list
    )

    # pydantic validators
    _version_validator = validator("version", pre=True, allow_reuse=True)(version_validator)

    @root_validator
    @classmethod
    def _add_derived_attributes(cls, values: dict[str, Any]) -> dict[str, Any]:
        # do not check entity_ids as the derived result can be an empty list
        derived_attributes = [
            values.get("primary_table_ids"),
            values.get("table_ids"),
            values.get("dtype"),
        ]
        if any(not x for x in derived_attributes):
            # only derive attributes if any of them is missing
            # extract table ids & entity ids from the graph
            graph_dict = values["internal_graph"]
            if isinstance(graph_dict, QueryGraphModel):
                graph_dict = graph_dict.dict(by_alias=True)
            graph = QueryGraph(**graph_dict)
            node_name = values["node_name"]
            values["primary_table_ids"] = graph.get_primary_table_ids(node_name=node_name)
            values["table_ids"] = graph.get_table_ids(node_name=node_name)
            values["entity_ids"] = graph.get_entity_ids(node_name=node_name)
            values["user_defined_function_ids"] = graph.get_user_defined_function_ids(
                node_name=node_name
            )

            # extract dtype from the graph
            node = graph.get_node_by_name(node_name)
            op_struct = graph.extract_operation_structure(node=node, keep_all_source_columns=True)
            if len(op_struct.aggregations) != 1:
                raise ValueError("Feature or target graph must have exactly one aggregation output")

            values["dtype"] = op_struct.aggregations[0].dtype
        return values

    @property
    def node(self) -> Node:
        """
        Retrieve node

        Returns
        -------
        Node
            Node object
        """

        return self.graph.get_node_by_name(self.node_name)

    @property
    def graph(self) -> QueryGraph:
        """
        Get the graph. If the graph is not loaded, load it first.

        Returns
        -------
        QueryGraph
            QueryGraph object
        """
        # TODO: make this a cached_property for pydantic v2
        if self._graph is None:
            if isinstance(self.internal_graph, QueryGraph):
                self._graph = self.internal_graph
            else:
                if isinstance(self.internal_graph, dict):
                    graph_dict = self.internal_graph
                else:
                    # for example, QueryGraphModel
                    graph_dict = self.internal_graph.dict(by_alias=True)
                self._graph = QueryGraph(**graph_dict)
        return self._graph

    def extract_pruned_graph_and_node(self, **kwargs: Any) -> tuple[QueryGraphModel, Node]:
        """
        Extract pruned graph and node

        Parameters
        ----------
        **kwargs: Any
            Additional keyword parameters

        Returns
        -------
        tuple[QueryGraph, Node]
            Pruned graph and node
        """
        _ = kwargs
        pruned_graph, node_name_map = self.graph.prune(target_node=self.node)
        mapped_node = pruned_graph.get_node_by_name(node_name_map[self.node.name])
        return pruned_graph, mapped_node

    def extract_operation_structure(
        self, keep_all_source_columns: bool = False
    ) -> GroupOperationStructure:
        """
        Extract feature or target operation structure based on query graph. This method is mainly
        used for deriving feature or target metadata used in feature/target info.

        Parameters
        ----------
        keep_all_source_columns: bool
            Whether to keep all source columns in the operation structure

        Returns
        -------
        GroupOperationStructure
        """
        # group the view columns by source columns & derived columns
        operation_structure = self.graph.extract_operation_structure(
            self.node, keep_all_source_columns=keep_all_source_columns
        )
        return operation_structure.to_group_operation_structure()

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        unique_constraints = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_BY_ID,
            ),
            UniqueValuesConstraint(
                fields=("name", "version"),
                conflict_fields_signature={"name": ["name"], "version": ["version"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_BY_ID,
            ),
        ]
        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("dtype"),
            pymongo.operations.IndexModel("version"),
            pymongo.operations.IndexModel("entity_ids"),
            pymongo.operations.IndexModel("table_ids"),
            pymongo.operations.IndexModel("primary_table_ids"),
            pymongo.operations.IndexModel("user_defined_function_ids"),
            pymongo.operations.IndexModel(
                [
                    ("name", pymongo.TEXT),
                    ("version", pymongo.TEXT),
                ],
            ),
        ]


class FeatureModel(BaseFeatureModel):
    """
    Model for Feature asset

    id: PydanticObjectId
        Feature id of the object
    name: str
        Feature name
    dtype: DBVarType
        Variable type of the feature
    graph: QueryGraph
        Graph contains steps of transformation to generate the feature
    node_name: str
        Node name of the graph which represent the feature
    tabular_source: TabularSource
        Tabular source used to construct this feature
    readiness: FeatureReadiness
        Feature readiness
    version: VersionIdentifier
        Feature version
    online_enabled: bool
        Whether to make this feature version online enabled
    definition: str
        Feature definition
    entity_ids: List[PydanticObjectId]
        Entity IDs used by the feature
    table_ids: List[PydanticObjectId]
        Table IDs used by the feature
    primary_table_ids: Optional[List[PydanticObjectId]]
        Primary table IDs of the feature (auto-derive from graph)
    feature_namespace_id: PydanticObjectId
        Feature namespace id of the object
    feature_list_ids: List[PydanticObjectId]
        FeatureList versions which use this feature version
    deployed_feature_list_ids: List[PydanticObjectId]
        Deployed FeatureList versions which use this feature version
    created_at: Optional[datetime]
        Datetime when the Feature was first saved
    updated_at: Optional[datetime]
        When the Feature get updated
    """

    readiness: FeatureReadiness = Field(allow_mutation=False, default=FeatureReadiness.DRAFT)
    online_enabled: bool = Field(allow_mutation=False, default=False)

    # ID related fields associated with this feature
    feature_namespace_id: PydanticObjectId = Field(allow_mutation=False, default_factory=ObjectId)
    feature_list_ids: List[PydanticObjectId] = Field(allow_mutation=False, default_factory=list)
    deployed_feature_list_ids: List[PydanticObjectId] = Field(
        allow_mutation=False, default_factory=list
    )
    aggregation_ids: List[str] = Field(allow_mutation=False, default_factory=list)
    aggregation_result_names: List[str] = Field(allow_mutation=False, default_factory=list)

    @root_validator
    @classmethod
    def _add_tile_derived_attributes(cls, values: dict[str, Any]) -> dict[str, Any]:
        # Each aggregation_id refers to a set of columns in a tile table. It is associated to a
        # specific scheduled tile task. An aggregation_id can produce multiple aggregation results
        # using different feature derivation windows.
        if values.get("aggregation_ids") and values.get("aggregation_result_names"):
            return values

        graph_dict = values["internal_graph"]
        if isinstance(graph_dict, QueryGraphModel):
            graph_dict = graph_dict.dict(by_alias=True)
        graph = QueryGraph(**graph_dict)
        node_name = values["node_name"]
        feature_store_type = graph.get_input_node(node_name).parameters.feature_store_details.type

        interpreter = GraphInterpreter(graph, feature_store_type)
        node = graph.get_node_by_name(node_name)
        tile_infos = interpreter.construct_tile_gen_sql(node, is_on_demand=False)

        aggregation_ids = []
        for info in tile_infos:
            aggregation_ids.append(info.aggregation_id)

        values["aggregation_ids"] = aggregation_ids

        values["aggregation_result_names"] = [
            query.result_name
            for query in get_online_store_precompute_queries(
                graph, graph.get_node_by_name(node_name), feature_store_type
            )
        ]
        return values

    class Settings(BaseFeatureModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "feature"
        indexes = BaseFeatureModel.Settings.indexes + [
            pymongo.operations.IndexModel("readiness"),
            pymongo.operations.IndexModel("online_enabled"),
            pymongo.operations.IndexModel("feature_namespace_id"),
            pymongo.operations.IndexModel("feature_list_ids"),
            pymongo.operations.IndexModel("deployed_feature_list_ids"),
            pymongo.operations.IndexModel("aggregation_ids"),
            pymongo.operations.IndexModel("aggregation_result_names"),
        ]
