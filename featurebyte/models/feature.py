"""
This module contains Feature related models
"""
from __future__ import annotations

from typing import Any, List, Optional

from bson.objectid import ObjectId
from pydantic import Field, StrictStr, root_validator, validator

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
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.metadata.operation import GroupOperationStructure


class FeatureReadiness(OrderedStrEnum):
    """Feature readiness"""

    DEPRECATED = "DEPRECATED"
    QUARANTINE = "QUARANTINE"
    DRAFT = "DRAFT"
    PRODUCTION_READY = "PRODUCTION_READY"


class DefaultVersionMode(StrEnum):
    """Default feature setting mode"""

    AUTO = "AUTO"
    MANUAL = "MANUAL"


class FrozenFeatureNamespaceModel(FeatureByteCatalogBaseDocumentModel):
    """
    FrozenFeatureNamespaceModel store all the attributes that are fixed after object construction.
    """

    dtype: DBVarType = Field(
        allow_mutation=False, description="database variable type for the feature"
    )
    entity_ids: List[PydanticObjectId] = Field(allow_mutation=False)
    tabular_data_ids: List[PydanticObjectId] = Field(allow_mutation=False)

    # pydantic validators
    _sort_ids_validator = validator("entity_ids", "tabular_data_ids", allow_reuse=True)(
        construct_sort_validator()
    )

    @root_validator(pre=True)
    @classmethod
    def _validate_tabular_data_ids(cls, values: dict[str, Any]) -> dict[str, Any]:
        # DEV-727: refactor event_data_ids to tabular_data_ids
        if "event_data_ids" in values:
            values["tabular_data_ids"] = values.pop("event_data_ids", [])
        return values

    class Settings:
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
    tabular_data_ids: List[PydanticObjectId]
        Tabular data IDs used for the feature
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
        allow_mutation=False, default=None, description="Feature Version"
    )
    entity_ids: List[PydanticObjectId] = Field(allow_mutation=False)
    tabular_data_ids: List[PydanticObjectId] = Field(allow_mutation=False)
    feature_namespace_id: PydanticObjectId = Field(allow_mutation=False, default_factory=ObjectId)
    feature_list_ids: List[PydanticObjectId] = Field(allow_mutation=False, default_factory=list)

    # pydantic validators
    _sort_ids_validator = validator("entity_ids", "tabular_data_ids", allow_reuse=True)(
        construct_sort_validator()
    )
    _version_validator = validator("version", pre=True, allow_reuse=True)(version_validator)

    @root_validator(pre=True)
    @classmethod
    def _validate_tabular_data_ids(cls, values: dict[str, Any]) -> dict[str, Any]:
        # DEV-727: refactor event_data_ids to tabular_data_ids
        if "event_data_ids" in values:
            values["tabular_data_ids"] = values.pop("event_data_ids", [])
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

    @root_validator(pre=True)
    @classmethod
    def _convert_graph_format(cls, values: dict[str, Any]) -> dict[str, Any]:
        # DEV-556: converted older record (graph) into a newer format
        if isinstance(values.get("graph"), dict):
            if isinstance(values.get("graph", {}).get("nodes"), dict):
                # in the old format, nodes is a dictionary but not a list
                graph: dict[str, Any] = {"nodes": [], "edges": []}
                for node in values["graph"]["nodes"].values():
                    graph["nodes"].append(node)
                for parent, children in values["graph"]["edges"].items():
                    for child in children:
                        graph["edges"].append({"source": parent, "target": child})
                values["graph"] = graph
            if isinstance(values.get("node"), dict):
                values["node_name"] = values["node"]["name"]
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

    class Settings:
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
    tabular_data_ids: List[PydanticObjectId]
        Tabular data IDs used for the feature version
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

    @validator("online_enabled", pre=True)
    @classmethod
    def _validate_online_enabled(cls, value: Optional[bool]) -> bool:
        # DEV-556: converted older record `None` value to `False`
        if value is None:
            return False
        return value


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
