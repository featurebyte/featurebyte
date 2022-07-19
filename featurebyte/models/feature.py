"""
This module contains Feature related models
"""
# pylint: disable=too-few-public-methods
from __future__ import annotations

from typing import List, Optional, Tuple

from datetime import datetime
from enum import Enum

from beanie import PydanticObjectId
from bson.objectid import ObjectId
from pydantic import BaseModel, Field, StrictStr

from featurebyte.common.model_util import get_version
from featurebyte.enum import DBVarType, OrderedStrEnum
from featurebyte.models.feature_store import FeatureStoreModel, TableDetails
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


class FeatureNameSpaceModel(BaseModel):
    """
    Feature set with the same feature name

    id: PydanticObjectId
        Feature namespace id
    name: str
        Feature name
    description: str
        Feature namespace descriptions applied to all features with the same family
    version_ids: List[PydanticObjectId]
        List of feature version id
    versions: List[FeatureVersionIdentifier]
        List of available feature version
    readiness: FeatureReadiness
        Aggregated readiness across all feature versions of the same feature namespace
    created_at: datetime
        Datetime when the FeatureNamespace was first saved or published
    default_version_id: PydanticObjectId
        Default feature version id
    default_version_mode: DefaultVersionMode
        Default feature version mode
    """

    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    description: Optional[StrictStr]
    version_ids: List[PydanticObjectId]
    versions: List[FeatureVersionIdentifier]
    readiness: FeatureReadiness
    created_at: datetime
    default_version_id: PydanticObjectId
    default_version_mode: DefaultVersionMode = Field(default=DefaultVersionMode.AUTO)

    class Config:
        """
        Configuration for FeatureNameSpace
        """

        use_enum_values = True
        json_encoders = {ObjectId: str}


class FeatureModel(BaseModel):
    """
    Model for Feature entity

    id: PydanticObjectId
        Feature id of the object
    name: str
        Feature name
    description: str
        Feature description specific to this feature version
    var_type: DBVarType
        Variable type of the feature
    lineage: Tuple[str, ...]
        Tuple of transformation step node names which affect the value of the feature
    row_index_lineage: Tuple[str, ...]
        Tuple of transformation step node names which affect the row number of the feature
    graph: QueryGraph
        Graph contains steps of transformation to generate the feature
    node: Node
        Node of the graph which represent the feature
    tabular_source: Tuple[FeatureStoreModel, TableDetails]
        Tabular source used to construct this feature
    readiness: Optional[FeatureReadiness]
        Feature readiness
    version: FeatureVersionIdentifier
        Feature version
    is_default: Optional[bool]
        Whether to this feature version default for the feature namespace
    online_enabled: Optional[bool]
        Whether to make this feature version online enabled
    event_data_ids: List[PydnaticObjectId]
        EventData IDs used for the feature version
    created_at: Optional[datetime]
        Datetime when the Feature was first saved or published
    parent_id: PydanticObjectId
        Parent feature id of the object
    """

    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id")
    name: Optional[StrictStr]
    description: Optional[StrictStr]
    var_type: DBVarType
    lineage: Tuple[StrictStr, ...]
    row_index_lineage: Tuple[StrictStr, ...]
    graph: QueryGraph
    node: Node
    tabular_source: Tuple[FeatureStoreModel, TableDetails]
    readiness: Optional[FeatureReadiness]
    version: FeatureVersionIdentifier = Field(default_factory=get_version)
    is_default: Optional[bool]
    online_enabled: Optional[bool]
    event_data_ids: List[PydanticObjectId] = Field(default_factory=list)
    created_at: Optional[datetime] = Field(default=None)
    parent_id: Optional[PydanticObjectId]

    class Config:
        """
        Configuration for Feature Data schema
        """

        use_enum_values = True
        json_encoders = {ObjectId: str}


class FeatureListModel(BaseModel):
    """
    Model for feature list entity

    id: PydanticObjectId
        FeatureList id of the object
    name: str
        Name of the feature list
    description: Optional[str]
        Description of the feature list
    features: List[Tuple[str, FeatureVersionIdentifier]]
        List of tuples which contain (feature name, feature version)
    readiness: FeatureReadiness
        Aggregated readiness of the features/feature classes
    status: FeatureListStatus
        FeatureList status
    version: FeatureListVersionIdentifier
        Feature list version
    created_at: Optional[datetime]
        Datetime when the FeatureList was first saved or published
    """

    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    description: Optional[StrictStr]
    features: List[Tuple[StrictStr, Optional[FeatureVersionIdentifier]]]
    readiness: Optional[FeatureReadiness]
    status: Optional[FeatureListStatus]
    version: Optional[FeatureListVersionIdentifier]
    created_at: Optional[datetime]

    class Config:
        """
        Configuration for FeatureListModel
        """

        use_enum_values = True
        json_encoders = {ObjectId: str}
