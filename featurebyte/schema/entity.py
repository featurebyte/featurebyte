"""
Entity API payload schema
"""
from __future__ import annotations

from typing import Any, List, Optional

from beanie import PydanticObjectId
from bson.objectid import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.entity import EntityModel
from featurebyte.routes.common.schema import BaseBriefInfo, BaseInfo, PaginationMixin
from featurebyte.schema.common.operation import DictProject, DictTransform


class EntityCreate(FeatureByteBaseModel):
    """
    Entity Creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    serving_name: StrictStr


class EntityList(PaginationMixin):
    """
    Paginated list of Entity
    """

    data: List[EntityModel]


class EntityUpdate(FeatureByteBaseModel):
    """
    Entity update schema
    """

    name: StrictStr


class EntityBriefInfo(BaseBriefInfo):
    """
    Entity brief info schema
    """

    serving_names: List[str]


class EntityBriefInfoList(PaginationMixin):
    """
    Paginated list of entity brief info
    """

    data: List[EntityBriefInfo]

    @classmethod
    def from_paginated_data(cls, paginated_data: dict[str, Any]) -> EntityBriefInfoList:
        entity_transform = DictTransform(
            rule={
                "__root__": DictProject(rule=["page", "page_size", "total"]),
                "data": DictProject(rule=("data", ["name", "serving_names"])),
            }
        )
        return EntityBriefInfoList(**entity_transform.transform(paginated_data))


class EntityInfo(EntityBriefInfo, BaseInfo):
    """
    Entity info schema
    """
