"""
Feature API payload scheme
"""
from typing import List, Optional, Tuple

from beanie import PydanticObjectId
from pydantic import BaseModel, Field, StrictStr

from featurebyte.enum import DBVarType
from featurebyte.models.feature import FeatureModel, FeatureNameSpaceModel, FeatureVersionIdentifier
from featurebyte.models.feature_store import FeatureStoreModel, TableDetails
from featurebyte.query_graph.graph import Node, QueryGraph
from featurebyte.routes.common.schema import PaginationMixin


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


class FeatureCreate(BaseModel):
    """
    Feature Creation schema
    """

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


class FeatureList(PaginationMixin):
    """
    Paginated list of Feature
    """

    data: List[Feature]


class FeatureUpdate(BaseModel):
    """
    Feature update schema
    """
