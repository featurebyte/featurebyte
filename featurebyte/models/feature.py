"""
This module contains Feature related models
"""
from __future__ import annotations

from typing import List, Optional, Tuple

from datetime import datetime
from enum import Enum

from pydantic import BaseModel

from featurebyte.enum import DBVarType
from featurebyte.models.feature_store import FeatureStoreModel, TableDetails
from featurebyte.query_graph.graph import Node, QueryGraph

FeatureVersionIdentifier = str
FeatureListVersionIdentifier = str


class FeatureReadiness(str, Enum):
    """Feature readiness"""

    PRODUCTION_READY = "PRODUCTION_READY"
    DRAFT = "DRAFT"
    QUARANTINE = "QUARANTINE"
    DEPRECATED = "DEPRECATED"


class FeatureListStatus(str, Enum):
    """FeatureList status"""

    PUBLISHED = "PUBLISHED"
    DRAFT = "DRAFT"
    EXPERIMENTAL = "EXPERIMENTAL"
    DEPRECATED = "DEPRECATED"


class DefaultVersionMode(str, Enum):
    """Default feature setting mode"""

    AUTO = "AUTO"
    MANUAL = "MANUAL"


class TileType(str, Enum):
    """Tile Type"""

    ONLINE = "ONLINE"
    OFFLINE = "OFFLINE"


class TileSpec(BaseModel):
    """
    Model for TileSpec

    tile_id: str
        hash value of tile id and name
    time_modulo_frequency_seconds: int
        time modulo seconds for the tile
    blind_spot_seconds: int
        blind spot seconds for the tile
    frequency_minute: int
        frequency minute for the tile
    tile_sql: str
        sql for tile generation
    column_names: str
        comma separated string of column names for the tile table
    """

    tile_id: str
    tile_sql: str
    column_names: str

    time_modulo_frequency_second: int
    blind_spot_second: int
    frequency_minute: int


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

    name: str
    description: Optional[str]
    versions: List[FeatureVersionIdentifier]
    readiness: FeatureReadiness
    created_at: datetime
    default_version: FeatureVersionIdentifier
    default_version_mode: DefaultVersionMode


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

    name: Optional[str]
    description: Optional[str]
    var_type: DBVarType
    lineage: Tuple[str, ...]
    row_index_lineage: Tuple[str, ...]
    graph: QueryGraph
    node: Node
    tabular_source: Tuple[FeatureStoreModel, TableDetails]
    readiness: Optional[FeatureReadiness]
    version: Optional[FeatureVersionIdentifier]
    tile_specs: Optional[List[TileSpec]]
    is_default: Optional[bool]
    created_at: Optional[datetime]


class FeatureListModel(BaseModel):
    """
    Model for feature list entity

    description: str
        Description of the feature list
    features: List[Tuple[str, FeatureVersionIdentifier]]
        List of tuples which contain (feature name, feature version)
    readiness: FeatureReadiness
        Aggregated readiness of the features/feature classes
    status: FeatureListStatus
        FeatureList status
    feature_list_version: FeatureListVersionIdentifier
        Feature list version
    created_at: Optional[datetime]
        Datetime when the FeatureList was first saved or published
    """

    name: str
    description: Optional[str]
    features: List[Tuple[str, FeatureVersionIdentifier]]
    readiness: Optional[FeatureReadiness]
    status: Optional[FeatureListStatus]
    feature_list_version: FeatureListVersionIdentifier
    created_at: Optional[datetime]
