"""
Exposure API payload schema
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, List, Optional

from bson import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import (
    FeatureByteBaseModel,
    NameStr,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.models.exposure import ExposureModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node import Node
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin
from featurebyte.schema.common.feature_or_target import ComputeRequest
from featurebyte.schema.info import EntityBriefInfoList, TableBriefInfoList


class ExposureCreate(FeatureByteBaseModel):
    """
    Exposure creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    graph: QueryGraph
    node_name: str
    tabular_source: TabularSource


class ExposureList(PaginationMixin):
    """
    Paginated list of Exposure
    """

    data: List[ExposureModel]


class ExposureInfo(FeatureByteBaseModel):
    """
    Exposure info
    """

    id: PydanticObjectId
    exposure_name: str
    entities: EntityBriefInfoList
    window: Optional[str] = Field(default=None)
    has_recipe: bool
    created_at: datetime
    updated_at: Optional[datetime] = Field(default=None)
    primary_table: TableBriefInfoList
    metadata: Any
    namespace_description: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)


class ComputeExposureRequest(ComputeRequest):
    """
    Compute exposure request schema
    """

    feature_store_id: PydanticObjectId
    graph: QueryGraph
    node_names: List[StrictStr]
    exposure_id: Optional[PydanticObjectId] = Field(default=None)

    @property
    def nodes(self) -> List[Node]:
        """
        Get nodes

        Returns
        -------
        List[Node]
        """
        return [self.graph.get_node_by_name(name) for name in self.node_names]


class ExposureServiceUpdate(BaseDocumentServiceUpdateSchema):
    """
    Exposure service update schema
    """

    name: Optional[NameStr] = Field(default=None)

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
