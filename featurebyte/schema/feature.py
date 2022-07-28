"""
Feature API payload schema
"""
from __future__ import annotations

from typing import List, Optional, Tuple

from beanie import PydanticObjectId
from pydantic import Field, StrictStr

from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature import FeatureModel, FeatureNameSpaceModel, FeatureVersionIdentifier
from featurebyte.models.feature_store import FeatureStoreIdentifier, TableDetails
from featurebyte.query_graph.graph import Node, QueryGraph


class Feature(FeatureModel):
    """
    Feature Document Model
    """

    user_id: Optional[PydanticObjectId]


class FeatureNameSpace(FeatureNameSpaceModel):
    """
    FeatureNameSpace Document Model
    """

    user_id: Optional[PydanticObjectId]


class FeatureCreate(FeatureByteBaseModel):
    """
    Feature Creation schema
    """

    id: PydanticObjectId = Field(alias="_id")
    name: StrictStr
    description: Optional[StrictStr]
    var_type: DBVarType
    lineage: Tuple[StrictStr, ...]
    row_index_lineage: Tuple[StrictStr, ...]
    graph: QueryGraph
    node: Node
    tabular_source: Tuple[FeatureStoreIdentifier, TableDetails]
    version: Optional[FeatureVersionIdentifier]
    event_data_ids: List[PydanticObjectId] = Field(min_items=1)
    parent_id: Optional[PydanticObjectId]
