"""
This module contains Feature related models
"""
from __future__ import annotations

from typing import Any, List, Optional

import pymongo
from bson.objectid import ObjectId
from pydantic import Field, StrictStr, root_validator, validator

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.validator import construct_sort_validator, version_validator
from featurebyte.enum import DBVarType, OrderedStrEnum, StrEnum
from featurebyte.models.base import (
    FeatureByteBaseModel,
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
    VersionIdentifier,
)
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.metadata.operation import GroupOperationStructure


class FeatureReadiness(OrderedStrEnum):
    """Feature readiness"""

    DEPRECATED = "DEPRECATED"
    DRAFT = "DRAFT"
    PUBLIC_DRAFT = "PUBLIC_DRAFT"
    PRODUCTION_READY = "PRODUCTION_READY"


class DefaultVersionMode(StrEnum):
    """
    Default feature setting mode.
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.DefaultVersionMode")

    AUTO = "AUTO", "Automatically select the version to use."
    MANUAL = "MANUAL", "Manually select the version to use."


class FrozenFeatureNamespaceModel(FeatureByteCatalogBaseDocumentModel):
    """
    FrozenFeatureNamespaceModel store all the attributes that are fixed after object construction.
    """

    dtype: DBVarType = Field(
        allow_mutation=False, description="database variable type for the feature"
    )
    entity_ids: List[PydanticObjectId] = Field(allow_mutation=False)
    table_ids: List[PydanticObjectId] = Field(allow_mutation=False)

    # pydantic validators
    _sort_ids_validator = validator("entity_ids", "table_ids", allow_reuse=True)(
        construct_sort_validator()
    )

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "feature_namespace"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=None,
            ),
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.RENAME,
            ),
        ]

        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("dtype"),
            pymongo.operations.IndexModel("entity_ids"),
            pymongo.operations.IndexModel("table_ids"),
        ]


class FeatureNamespaceModel(FrozenFeatureNamespaceModel):
    """
    Feature set with the same feature name

    id: PydanticObjectId
        Feature namespace id
    name: str
        Feature name
    dtype: DBVarType
        Variable type of the feature
    feature_ids: List[PydanticObjectId]
        List of feature version id
    online_enabled_feature_ids: List[PydanticObjectId]
        List of online enabled feature version id
    readiness: FeatureReadiness
        Aggregated readiness across all feature versions of the same feature namespace
    created_at: datetime
        Datetime when the FeatureNamespace was first saved or published
    default_feature_id: PydanticObjectId
        Default feature version id
    default_version_mode: DefaultVersionMode
        Default feature version mode
    entity_ids: List[PydanticObjectId]
        Entity IDs used by the feature
    table_ids: List[PydanticObjectId]
        Table IDs used by the feature
    """

    feature_ids: List[PydanticObjectId] = Field(allow_mutation=False)
    online_enabled_feature_ids: List[PydanticObjectId] = Field(
        allow_mutation=False, default_factory=list
    )
    readiness: FeatureReadiness = Field(allow_mutation=False)
    default_feature_id: PydanticObjectId = Field(allow_mutation=False)
    default_version_mode: DefaultVersionMode = Field(
        default=DefaultVersionMode.AUTO, allow_mutation=False
    )

    # pydantic validators
    _sort_feature_ids_validator = validator("feature_ids", allow_reuse=True)(
        construct_sort_validator()
    )

    class Settings(FrozenFeatureNamespaceModel.Settings):
        """
        MongoDB settings
        """

        indexes = FrozenFeatureNamespaceModel.Settings.indexes + [
            pymongo.operations.IndexModel("feature_ids"),
            pymongo.operations.IndexModel("online_enabled_feature_ids"),
            pymongo.operations.IndexModel("readiness"),
            pymongo.operations.IndexModel("default_feature_id"),
            pymongo.operations.IndexModel(
                [
                    ("name", pymongo.TEXT),
                ],
            ),
        ]


class FrozenFeatureModel(FeatureByteCatalogBaseDocumentModel):
    """
    FrozenFeatureModel store all the attributes that are fixed after object construction.
    """

    dtype: DBVarType = Field(
        allow_mutation=False, description="database variable type for the feature"
    )
    graph: QueryGraph = Field(allow_mutation=False)
    node_name: str
    tabular_source: TabularSource = Field(allow_mutation=False)
    version: VersionIdentifier = Field(
        allow_mutation=False,
        default=None,
        description="Returns the version identifier of a Feature object.",
    )
    entity_ids: List[PydanticObjectId] = Field(allow_mutation=False)
    table_ids: List[PydanticObjectId] = Field(allow_mutation=False, default_factory=list)
    primary_table_ids: List[PydanticObjectId] = Field(allow_mutation=False, default_factory=list)
    feature_namespace_id: PydanticObjectId = Field(allow_mutation=False, default_factory=ObjectId)
    feature_list_ids: List[PydanticObjectId] = Field(allow_mutation=False, default_factory=list)

    # pydantic validators
    _sort_ids_validator = validator("entity_ids", allow_reuse=True)(construct_sort_validator())
    _version_validator = validator("version", pre=True, allow_reuse=True)(version_validator)

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

    @root_validator
    @classmethod
    def _add_derived_attributes(cls, values: dict[str, Any]) -> dict[str, Any]:
        if values.get("graph") and values.get("node_name"):
            graph = values["graph"]
            if isinstance(graph, dict):
                graph = QueryGraphModel(**dict(graph))

            node_name = values["node_name"]
            node = graph.get_node_by_name(node_name)
            primary_input_nodes = graph.get_primary_input_nodes(node_name=node_name)
            values["primary_table_ids"] = sorted(
                node.parameters.id for node in primary_input_nodes if node.parameters.id
            )
            values["table_ids"] = sorted(
                node.parameters.id
                for node in graph.iterate_nodes(target_node=node, node_type=NodeType.INPUT)
                if node.parameters.id
            )
        return values

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
        pruned_graph, node_name_map = self.graph.prune(target_node=self.node, aggressive=True)
        mapped_node = pruned_graph.get_node_by_name(node_name_map[self.node.name])
        return pruned_graph, mapped_node

    def extract_operation_structure(self) -> GroupOperationStructure:
        """
        Extract feature operation structure based on query graph.

        Returns
        -------
        GroupOperationStructure
        """
        # group the view columns by source columns & derived columns
        operation_structure = self.graph.extract_operation_structure(self.node)
        return operation_structure.to_group_operation_structure()

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "feature"
        unique_constraints: List[UniqueValuesConstraint] = [
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
            pymongo.operations.IndexModel("feature_namespace_id"),
            pymongo.operations.IndexModel("feature_list_ids"),
        ]


class FeatureModel(FrozenFeatureModel):
    """
    Model for Feature entity

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
    deployed_feature_list_ids: List[PydanticObjectId] = Field(
        allow_mutation=False, default_factory=list
    )
    definition: Optional[str] = Field(
        allow_mutation=False, default=None, description="Feature Definition"
    )

    class Settings(FrozenFeatureModel.Settings):
        """
        MongoDB settings
        """

        indexes = FrozenFeatureModel.Settings.indexes + [
            pymongo.operations.IndexModel("readiness"),
            pymongo.operations.IndexModel("online_enabled"),
            pymongo.operations.IndexModel("deployed_feature_list_ids"),
            pymongo.operations.IndexModel(
                [
                    ("name", pymongo.TEXT),
                    ("version", pymongo.TEXT),
                ],
            ),
        ]


class FeatureSignature(FeatureByteBaseModel):
    """
    FeatureSignature class used in FeatureList object

    id: PydanticObjectId
        Feature id of the object
    name: str
        Name of the feature
    version: VersionIdentifier
        Feature version
    """

    id: PydanticObjectId
    name: Optional[StrictStr]
    version: VersionIdentifier
