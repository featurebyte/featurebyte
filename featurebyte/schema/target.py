"""
Target API payload schema
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from bson import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import (
    FeatureByteBaseModel,
    NameStr,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.models.target import TargetModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node import Node
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin
from featurebyte.schema.common.feature_or_target import ComputeRequest
from featurebyte.schema.info import EntityBriefInfoList, TableBriefInfoList


class TargetCreate(FeatureByteBaseModel):
    """
    Target creation schema
    """

    id: PydanticObjectId | None = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    graph: QueryGraph
    node_name: str
    tabular_source: TabularSource


class TargetList(PaginationMixin):
    """
    Paginated list of Target
    """

    data: list[TargetModel]


class TargetInfo(FeatureByteBaseModel):
    """
    Target info
    """

    id: PydanticObjectId
    target_name: str
    entities: EntityBriefInfoList
    window: str | None = Field(default=None)
    has_recipe: bool
    created_at: datetime
    updated_at: datetime | None = Field(default=None)
    primary_table: TableBriefInfoList
    metadata: Any
    namespace_description: str | None = Field(default=None)
    description: str | None = Field(default=None)


class ComputeTargetRequest(ComputeRequest):
    """
    Compute target request schema
    """

    feature_store_id: PydanticObjectId
    graph: QueryGraph
    node_names: list[StrictStr]
    target_id: PydanticObjectId | None = Field(default=None)

    @property
    def nodes(self) -> list[Node]:
        """
        Get feature nodes

        Returns
        -------
        List[Node]
        """
        return [self.graph.get_node_by_name(name) for name in self.node_names]


class TargetServiceUpdate(BaseDocumentServiceUpdateSchema):
    """
    Target service update schema
    """

    name: NameStr | None = Field(default=None)

    class Settings(BaseDocumentServiceUpdateSchema.Settings):
        """
        Unique constraints checking
        """

        unique_constraints: list[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]
