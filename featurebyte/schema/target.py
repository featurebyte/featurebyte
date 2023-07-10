"""
Target API payload schema
"""
from __future__ import annotations

from typing import Any, List, Optional

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
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin
from featurebyte.schema.info import EntityBriefInfoList


class TargetCreate(FeatureByteBaseModel):
    """
    Target creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    graph: QueryGraph
    node_name: str
    tabular_source: TabularSource


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


class TableMetadata(FeatureByteBaseModel):
    """
    Table metadata
    """

    name: StrictStr
    data_type: StrictStr


class InputData(FeatureByteBaseModel):
    """
    Input data
    """

    main_data: TableMetadata
    other_data: Optional[List[TableMetadata]] = None


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
    input_data: InputData
    metadata: Any


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
