"""
This module contains Feature related models
"""
# pylint: disable=too-few-public-methods
from __future__ import annotations

from typing import List, Optional, Tuple

from enum import Enum

from beanie import PydanticObjectId
from bson.objectid import ObjectId
from pydantic import Field, StrictStr, validator

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


class FeatureReadiness(OrderedStrEnum):
    """Feature readiness"""

    DEPRECATED = "DEPRECATED"
    QUARANTINE = "QUARANTINE"
    DRAFT = "DRAFT"
    PRODUCTION_READY = "PRODUCTION_READY"


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
    dtype: DBVarType
        Variable type of the feature
    feature_ids: List[PydanticObjectId]
        List of feature version id
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
    event_data_ids: List[PydanticObjectId]
        EventData IDs used for the feature version
    """

    dtype: DBVarType = Field(allow_mutation=False)
    feature_ids: List[PydanticObjectId] = Field(allow_mutation=False)
    readiness: FeatureReadiness = Field(allow_mutation=False)
    default_feature_id: PydanticObjectId = Field(allow_mutation=False)
    default_version_mode: DefaultVersionMode = Field(
        default=DefaultVersionMode.AUTO, allow_mutation=False
    )
    entity_ids: List[PydanticObjectId] = Field(allow_mutation=False)
    event_data_ids: List[PydanticObjectId] = Field(allow_mutation=False)

    @validator("feature_ids", "entity_ids", "event_data_ids")
    @classmethod
    def _validate_ids(cls, value: List[ObjectId]) -> List[ObjectId]:
        # make sure list of ids always sorted
        return sorted(value)

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
    dtype: DBVarType
        Variable type of the feature
    row_index_lineage: Tuple[str, ...]
        Tuple of transformation step node names which affect the row number of the feature
    graph: QueryGraph
        Graph contains steps of transformation to generate the feature
    node: Node
        Node of the graph which represent the feature
    tabular_source: TabularSource
        Tabular source used to construct this feature
    readiness: FeatureReadiness
        Feature readiness
    version: FeatureVersionIdentifier
        Feature version
    is_default: Optional[bool]
        Whether to this feature version default for the feature namespace
    online_enabled: Optional[bool]
        Whether to make this feature version online enabled
    entity_ids: List[PydanticObjectId]
        Entity IDs used by the feature
    event_data_ids: List[PydanticObjectId]
        EventData IDs used for the feature version
    created_at: Optional[datetime]
        Datetime when the Feature was first saved or published
    feature_namespace_id: PydanticObjectId
        Feature namespace id of the object
    """

    dtype: DBVarType = Field(allow_mutation=False)
    row_index_lineage: Tuple[StrictStr, ...] = Field(allow_mutation=False)
    graph: QueryGraph = Field(allow_mutation=False)
    node: Node = Field(allow_mutation=False)
    tabular_source: TabularSource = Field(allow_mutation=False)
    readiness: FeatureReadiness = Field(allow_mutation=False, default=FeatureReadiness.DRAFT)
    version: FeatureVersionIdentifier = Field(default_factory=get_version, allow_mutation=False)
    online_enabled: Optional[bool] = Field(allow_mutation=False)
    entity_ids: List[PydanticObjectId] = Field(allow_mutation=False)
    event_data_ids: List[PydanticObjectId] = Field(allow_mutation=False)
    feature_namespace_id: PydanticObjectId = Field(allow_mutation=False, default_factory=ObjectId)
    feature_list_ids: List[PydanticObjectId] = Field(allow_mutation=False, default_factory=list)

    @validator("entity_ids", "event_data_ids")
    @classmethod
    def _validate_ids(cls, value: List[ObjectId]) -> List[ObjectId]:
        # make sure list of ids always sorted
        return sorted(value)

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
