"""
This module contains Feature related models
"""
# pylint: disable=too-few-public-methods
from __future__ import annotations

from typing import List, Optional, Tuple

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, StrictStr

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


class FeatureNameSpace(BaseModel):
    """
    Feature set with the same feature name

    name: str
        Feature name
    description: str
        Feature family descriptions applied to all features with the same family
    versions: List[FeatureVersionIdentifier]
        List of available feature version
    """

    name: StrictStr
    description: Optional[StrictStr]
    versions: List[FeatureVersionIdentifier]
    readiness: FeatureReadiness
    created_at: datetime
    default_version: FeatureVersionIdentifier
    default_version_mode: DefaultVersionMode

    class Config:
        """
        Configuration for FeatureNameSpace
        """

        use_enum_values = True


class FeatureModel(BaseModel):
    """
    Model for Feature entity

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
    created_at: Optional[datetime]
        Datetime when the Feature was first saved or published
    """

    name: Optional[StrictStr]
    description: Optional[StrictStr]
    var_type: DBVarType
    lineage: Tuple[StrictStr, ...]
    row_index_lineage: Tuple[StrictStr, ...]
    graph: QueryGraph
    node: Node
    tabular_source: Tuple[FeatureStoreModel, TableDetails]
    readiness: Optional[FeatureReadiness]
    version: Optional[FeatureVersionIdentifier]
    is_default: Optional[bool]
    online_enabled: Optional[bool]
    created_at: Optional[datetime]

    class Config:
        """
        Configuration for FeatureModel
        """

        use_enum_values = True


class FeatureListModel(BaseModel):
    """
    Model for feature list entity

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

    name: StrictStr
    description: Optional[StrictStr]
    features: List[Tuple[StrictStr, Optional[FeatureVersionIdentifier]]]
    readiness: Optional[FeatureReadiness]
    status: Optional[FeatureListStatus]
    version: FeatureListVersionIdentifier
    created_at: Optional[datetime]

    class Config:
        """
        Configuration for FeatureListModel
        """

        use_enum_values = True
