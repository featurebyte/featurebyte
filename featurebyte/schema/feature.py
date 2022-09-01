"""
Feature API payload schema
"""
from __future__ import annotations

from typing import List, Optional, Tuple

from beanie import PydanticObjectId
from bson.objectid import ObjectId
from pydantic import Field, StrictStr

from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature import FeatureModel, FeatureVersionIdentifier
from featurebyte.models.feature_store import TabularSource
from featurebyte.query_graph.graph import Node, QueryGraph
from featurebyte.routes.common.schema import PaginationMixin


class FeatureCreate(FeatureByteBaseModel):
    """
    Feature Creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    dtype: DBVarType
    row_index_lineage: Tuple[StrictStr, ...]
    graph: QueryGraph
    node: Node
    tabular_source: TabularSource
    version: Optional[FeatureVersionIdentifier]
    event_data_ids: List[PydanticObjectId] = Field(min_items=1)
    entity_ids: List[PydanticObjectId] = Field(min_items=1)
    feature_namespace_id: Optional[PydanticObjectId]


class FeatureList(PaginationMixin):
    """
    Paginated list of Entity
    """

    data: List[FeatureModel]
