"""
This module contains Feature related models
"""
# pylint: disable=too-few-public-methods
from __future__ import annotations

from typing import List, Optional, Tuple

from enum import Enum

from beanie import PydanticObjectId
from bson.objectid import ObjectId
from pydantic import Field, StrictStr

from featurebyte.common.model_util import get_version
from featurebyte.enum import DBVarType, OrderedStrEnum
from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.models.feature_store import TabularSource
from featurebyte.query_graph.graph import Node, QueryGraph

FeatureVersionIdentifier = StrictStr
FeatureListVersionIdentifier = StrictStr


class FeatureReadiness(OrderedStrEnum):
    """Feature readiness"""

    DEPRECATED = "DEPRECATED"
    QUARANTINE = "QUARANTINE"
    DRAFT = "DRAFT"
    PRODUCTION_READY = "PRODUCTION_READY"


class FeatureListStatus(OrderedStrEnum):
    """FeatureList status"""

    DEPRECATED = "DEPRECATED"
    EXPERIMENTAL = "EXPERIMENTAL"
    DRAFT = "DRAFT"
    PUBLISHED = "PUBLISHED"


class DefaultVersionMode(str, Enum):
    """Default feature setting mode"""

    AUTO = "AUTO"
    MANUAL = "MANUAL"


class FeatureNamespaceModel(FeatureByteBaseDocumentModel):
    """
    Feature set with the same feature name

    id: PydanticObjectId
        Feature namespace id
    name: str
        Feature name
    version_ids: List[PydanticObjectId]
        List of feature version id
    readiness: FeatureReadiness
        Aggregated readiness across all feature versions of the same feature namespace
    created_at: datetime
        Datetime when the FeatureNamespace was first saved or published
    default_version_id: PydanticObjectId
        Default feature version id
    default_version_mode: DefaultVersionMode
        Default feature version mode
    """

    version_ids: List[PydanticObjectId]
    readiness: FeatureReadiness
    default_version_id: PydanticObjectId
    default_version_mode: DefaultVersionMode = Field(default=DefaultVersionMode.AUTO)

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


class FeatureModel(FeatureByteBaseDocumentModel):
    """
    Model for Feature entity

    id: PydanticObjectId
        Feature id of the object
    name: str
        Feature name
    var_type: DBVarType
        Variable type of the feature
    row_index_lineage: Tuple[str, ...]
        Tuple of transformation step node names which affect the row number of the feature
    graph: QueryGraph
        Graph contains steps of transformation to generate the feature
    node: Node
        Node of the graph which represent the feature
    tabular_source: TabularSource
        Tabular source used to construct this feature
    readiness: Optional[FeatureReadiness]
        Feature readiness
    version: FeatureVersionIdentifier
        Feature version
    is_default: Optional[bool]
        Whether to this feature version default for the feature namespace
    online_enabled: Optional[bool]
        Whether to make this feature version online enabled
    event_data_ids: List[PydanticObjectId]
        EventData IDs used for the feature version
    created_at: Optional[datetime]
        Datetime when the Feature was first saved or published
    feature_namespace_id: PydanticObjectId
        Feature namespace id of the object
    """

    var_type: DBVarType = Field(allow_mutation=False)
    row_index_lineage: Tuple[StrictStr, ...] = Field(allow_mutation=False)
    graph: QueryGraph = Field(allow_mutation=False)
    node: Node = Field(allow_mutation=False)
    tabular_source: TabularSource = Field(allow_mutation=False)
    readiness: Optional[FeatureReadiness] = Field(allow_mutation=False)
    version: FeatureVersionIdentifier = Field(default_factory=get_version, allow_mutation=False)
    online_enabled: Optional[bool] = Field(allow_mutation=False)
    event_data_ids: List[PydanticObjectId] = Field(default_factory=list, allow_mutation=False)
    feature_namespace_id: PydanticObjectId = Field(allow_mutation=False, default_factory=ObjectId)

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


class FeatureSignature(FeatureByteBaseModel):
    """
    FeatureSignature class used in FeatureList object

    id: PydanticObjectId
        Feature id of the object
    name: str
        Name of the feature
    version: FeatureVersionIdentifier
        Feature version
    """

    id: PydanticObjectId
    name: Optional[StrictStr]
    version: FeatureVersionIdentifier


class FeatureListModel(FeatureByteBaseDocumentModel):
    """
    Model for feature list entity

    id: PydanticObjectId
        FeatureList id of the object
    name: str
        Name of the feature list
    feature_ids: List[PydanticObjectId]
        List of feature IDs
    readiness: FeatureReadiness
        Aggregated readiness of the features/feature classes
    status: FeatureListStatus
        FeatureList status
    version: FeatureListVersionIdentifier
        Feature list version
    created_at: Optional[datetime]
        Datetime when the FeatureList was first saved or published
    """

    feature_ids: List[PydanticObjectId] = Field(default_factory=list)
    readiness: Optional[FeatureReadiness] = Field(allow_mutation=False)
    status: Optional[FeatureListStatus] = Field(allow_mutation=False)
    version: Optional[FeatureListVersionIdentifier] = Field(allow_mutation=False)

    class Settings:
        """
        MongoDB settings
        """

        collection_name: str = "feature_list"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
            UniqueValuesConstraint(
                fields=("feature_ids",),
                conflict_fields_signature={"feature_ids": ["feature_ids"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]
