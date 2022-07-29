"""
Feature API payload scheme
"""
from __future__ import annotations

from typing import List, Optional, Tuple

from beanie import PydanticObjectId
from pydantic import Field, StrictStr

from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature import FeatureVersionIdentifier
from featurebyte.models.feature_store import FeatureStoreModel, TableDetails
from featurebyte.query_graph.graph import Node, QueryGraph


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
    tabular_source: Tuple[FeatureStoreModel, TableDetails]
    version: Optional[FeatureVersionIdentifier]
    event_data_ids: List[PydanticObjectId] = Field(min_items=1)
    parent_id: Optional[PydanticObjectId]
