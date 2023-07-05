"""
Target API payload schema
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from datetime import datetime

from bson.objectid import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import (
    FeatureByteBaseModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.models.target import TargetModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin
from featurebyte.schema.info import EntityBriefInfoList


class TargetCreate(FeatureByteBaseModel):
    """
    Target creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    graph: Optional[QueryGraph]
    node_name: Optional[str]
    horizon: Optional[str]
    entity_ids: Optional[List[PydanticObjectId]]
    target_namespace_id: Optional[PydanticObjectId] = Field(default_factory=ObjectId)


class TargetList(PaginationMixin):
    """
    Paginated list of Target
    """

    data: List[TargetModel]


class TargetUpdate(FeatureByteBaseModel):
    """
    Target update schema
    """

    name: StrictStr


class TargetInfo(FeatureByteBaseModel):
    """
    Target info
    """

    id: PydanticObjectId
    target_name: str
    entities: EntityBriefInfoList
    horizon: Optional[str]
    has_recipe: bool
    created_at: datetime
    updated_at: Optional[datetime]


class TargetPreview(FeatureByteBaseModel):
    """
    Feature Preview schema
    """

    graph: QueryGraph
    node_name: str
    feature_store_name: StrictStr
    point_in_time_and_serving_name_list: List[Dict[str, Any]] = Field(min_items=1, max_items=50)


class TargetServiceUpdate(BaseDocumentServiceUpdateSchema):
    """
    Target service update schema
    """

    name: Optional[str]

    class Settings(BaseDocumentServiceUpdateSchema.Settings):
        """
        Unique constraints checking
        """

        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]
